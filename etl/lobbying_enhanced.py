#!/usr/bin/env python3
"""
Enhanced ETL script for Canadian lobbying registrations data.
Supports automatic fallback: Local PostgreSQL → Remote PostgreSQL → REST API.
"""

import os
import sys
import re
import io
import csv
import json
import zipfile
import tempfile
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any, List, Optional, Tuple
from urllib.request import urlretrieve
from urllib.error import URLError
from pathlib import Path
from enum import Enum

import psycopg
import requests


# Data source URL from Commissioner of Lobbying Canada
LOBBYING_DATA_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"

# Date threshold for filtering (last 2 years)
CUTOFF_DATE = datetime.now() - timedelta(days=730)

# Batch size for REST API insertions
BATCH_SIZE = 1000


class ConnectionMethod(Enum):
    LOCAL_POSTGRES = "local_postgres"
    REMOTE_POSTGRES = "remote_postgres"
    REST_API = "rest_api"


def load_env_file() -> None:
    """Load environment variables from .env file if it exists."""
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value


def snake_case(name: str) -> str:
    """Convert column name to snake_case."""
    name = re.sub(r'[^\w\s]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = name.lower()
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def download_lobbying_data() -> str:
    """Download the lobbying data ZIP file to a temporary location."""
    print("Downloading lobbying data...")
    
    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        temp_file.close()
        
        urlretrieve(LOBBYING_DATA_URL, temp_file.name)
        print(f"✓ Downloaded to {temp_file.name}")
        return temp_file.name
    
    except URLError as e:
        print(f"✗ Download failed: {e}")
        sys.exit(1)


def extract_primary_csv(zip_path: str) -> tuple[str, list[str]]:
    """Extract the primary registrations CSV and return headers."""
    print("Extracting CSV from ZIP archive...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        
        if not csv_files:
            print("✗ No CSV files found in archive")
            sys.exit(1)
        
        primary_csv = csv_files[0]
        print(f"✓ Found primary CSV: {primary_csv}")
        
        temp_csv = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
        temp_csv.close()
        
        with zip_ref.open(primary_csv) as source:
            with open(temp_csv.name, 'wb') as target:
                target.write(source.read())
        
        # Read headers with encoding detection
        for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
            try:
                with open(temp_csv.name, 'r', encoding=encoding) as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    snake_headers = [snake_case(h) for h in headers]
                    print(f"✓ Using encoding: {encoding}")
                    break
            except UnicodeDecodeError:
                continue
        else:
            print("✗ Could not determine file encoding")
            sys.exit(1)
        
        print(f"✓ Extracted CSV with {len(snake_headers)} columns")
        return temp_csv.name, snake_headers


def filter_recent_rows(csv_path: str, headers: list[str]) -> Iterator[list[str]]:
    """Filter CSV rows to only include registrations from the last 2 years."""
    print(f"Filtering data to last 2 years (since {CUTOFF_DATE.strftime('%Y-%m-%d')})...")
    
    # Try to find a date column
    date_col_idx = None
    for i, header in enumerate(headers):
        if any(date_term in header.lower() for date_term in ['date', 'created', 'registered', 'filed']):
            date_col_idx = i
            break
    
    # Try different encodings to read the CSV file
    for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
        try:
            if date_col_idx is None:
                print("⚠ No date column found, returning all rows")
                with open(csv_path, 'r', encoding=encoding) as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header
                    yield from reader
                return
            
            filtered_count = 0
            total_count = 0
            
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                for row in reader:
                    total_count += 1
                    
                    if len(row) > date_col_idx:
                        date_str = row[date_col_idx]
                        try:
                            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                                try:
                                    row_date = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                yield row
                                filtered_count += 1
                                continue
                            
                            if row_date >= CUTOFF_DATE:
                                yield row
                                filtered_count += 1
                                
                        except (ValueError, IndexError):
                            yield row
                            filtered_count += 1
            
            print(f"✓ Filtered {filtered_count:,} rows from {total_count:,} total rows")
            return
            
        except UnicodeDecodeError:
            continue
    
    print("✗ Could not read CSV file with any encoding")
    sys.exit(1)


class ConnectionManager:
    """Manages database connections with automatic fallback."""
    
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            print("✗ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables")
            sys.exit(1)
        
        self.host = self.supabase_url.replace('https://', '').replace('http://', '')
        self.connection_method = None
        self.postgres_conn = None
        self.rest_client = None
    
    def test_local_postgres(self) -> bool:
        """Test connection to local Supabase PostgreSQL."""
        try:
            conn_string = f"postgresql://postgres:postgres@localhost:54322/postgres"
            with psycopg.connect(conn_string, connect_timeout=5) as conn:
                print("✓ Local PostgreSQL connection successful")
                return True
        except Exception as e:
            print(f"✗ Local PostgreSQL connection failed: {e}")
            return False
    
    def test_remote_postgres(self) -> bool:
        """Test connection to remote Supabase PostgreSQL."""
        for port in [6543, 5432]:
            try:
                conn_string = f"postgresql://postgres:{self.supabase_key}@{self.host}:{port}/postgres?sslmode=require"
                with psycopg.connect(conn_string, connect_timeout=5) as conn:
                    print(f"✓ Remote PostgreSQL connection successful on port {port}")
                    return True
            except Exception as e:
                print(f"✗ Remote PostgreSQL connection failed on port {port}: {e}")
                continue
        return False
    
    def test_rest_api(self) -> bool:
        """Test connection to Supabase REST API."""
        try:
            headers = {
                'apikey': self.supabase_key,
                'Authorization': f'Bearer {self.supabase_key}',
                'Content-Type': 'application/json'
            }
            response = requests.get(f"{self.supabase_url}/rest/v1/", headers=headers, timeout=10)
            if response.status_code == 200:
                print("✓ REST API connection successful")
                return True
            else:
                print(f"✗ REST API connection failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ REST API connection failed: {e}")
            return False
    
    def establish_connection(self) -> ConnectionMethod:
        """Establish connection using the first available method."""
        print("Testing database connections...")
        
        # Try local PostgreSQL first
        if self.test_local_postgres():
            self.connection_method = ConnectionMethod.LOCAL_POSTGRES
            conn_string = f"postgresql://postgres:postgres@localhost:54322/postgres"
            self.postgres_conn = psycopg.connect(conn_string, connect_timeout=10)
            return ConnectionMethod.LOCAL_POSTGRES
        
        # Try remote PostgreSQL
        if self.test_remote_postgres():
            self.connection_method = ConnectionMethod.REMOTE_POSTGRES
            for port in [6543, 5432]:
                try:
                    conn_string = f"postgresql://postgres:{self.supabase_key}@{self.host}:{port}/postgres?sslmode=require"
                    self.postgres_conn = psycopg.connect(conn_string, connect_timeout=10)
                    break
                except:
                    continue
            return ConnectionMethod.REMOTE_POSTGRES
        
        # Fall back to REST API
        if self.test_rest_api():
            self.connection_method = ConnectionMethod.REST_API
            self.rest_client = SupabaseRESTClient(self.supabase_url, self.supabase_key)
            return ConnectionMethod.REST_API
        
        print("✗ All connection methods failed")
        sys.exit(1)
    
    def create_table(self, headers: list[str]) -> None:
        """Create the lobby_staging table."""
        if self.connection_method in [ConnectionMethod.LOCAL_POSTGRES, ConnectionMethod.REMOTE_POSTGRES]:
            self._create_table_postgres(headers)
        else:
            self._create_table_rest(headers)
    
    def _create_table_postgres(self, headers: list[str]) -> None:
        """Create table using PostgreSQL connection."""
        print("Creating lobby_staging table...")
        
        columns = [f"{header} TEXT" for header in headers]
        columns_sql = ",\n    ".join(columns)
        
        create_table_sql = f"""
        DROP TABLE IF EXISTS lobby_staging;
        CREATE TABLE lobby_staging (
            {columns_sql}
        );
        CREATE INDEX IF NOT EXISTS idx_lobby_staging_reg_id ON lobby_staging(reg_id_enr);
        CREATE INDEX IF NOT EXISTS idx_lobby_staging_country ON lobby_staging(country_pays);
        """
        
        with self.postgres_conn.cursor() as cur:
            cur.execute(create_table_sql)
            self.postgres_conn.commit()
        
        print("✓ Created lobby_staging table with indexes")
    
    def _create_table_rest(self, headers: list[str]) -> None:
        """Create table using REST API (requires manual creation)."""
        if self.rest_client.table_exists("lobby_staging"):
            print("✓ lobby_staging table already exists")
            return
        
        print("✗ lobby_staging table doesn't exist")
        print("\nTo create the table, run this SQL in your Supabase SQL editor:")
        print("=" * 60)
        
        columns_sql = ",\n    ".join([f"{col} TEXT" for col in headers])
        create_sql = f"""
CREATE TABLE lobby_staging (
    {columns_sql}
);
CREATE INDEX idx_lobby_staging_reg_id ON lobby_staging(reg_id_enr);
CREATE INDEX idx_lobby_staging_country ON lobby_staging(country_pays);
"""
        print(create_sql)
        print("=" * 60)
        print("\nAfter creating the table, run this script again.")
        sys.exit(1)
    
    def insert_data(self, csv_path: str, headers: list[str]) -> None:
        """Insert data using the established connection method."""
        if self.connection_method in [ConnectionMethod.LOCAL_POSTGRES, ConnectionMethod.REMOTE_POSTGRES]:
            self._insert_data_postgres(csv_path, headers)
        else:
            self._insert_data_rest(csv_path, headers)
    
    def _insert_data_postgres(self, csv_path: str, headers: list[str]) -> None:
        """Insert data using PostgreSQL COPY FROM STDIN."""
        print("Streaming data using PostgreSQL COPY...")
        
        with self.postgres_conn.cursor() as cur:
            copy_sql = f"COPY lobby_staging ({', '.join(headers)}) FROM STDIN WITH CSV"
            
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            
            row_count = 0
            for row in filter_recent_rows(csv_path, headers):
                writer.writerow(row)
                row_count += 1
                
                # Flush buffer periodically
                if row_count % 10000 == 0:
                    buffer.seek(0)
                    cur.copy(copy_sql, buffer)
                    buffer.seek(0)
                    buffer.truncate(0)
                    print(f"  Streamed {row_count:,} rows...")
            
            # Final flush
            if buffer.tell() > 0:
                buffer.seek(0)
                cur.copy(copy_sql, buffer)
            
            self.postgres_conn.commit()
            print(f"✓ Successfully streamed {row_count:,} rows using PostgreSQL COPY")
    
    def _insert_data_rest(self, csv_path: str, headers: list[str]) -> None:
        """Insert data using REST API in batches."""
        print("Streaming data using REST API...")
        
        batch = []
        row_count = 0
        
        for row in filter_recent_rows(csv_path, headers):
            row_dict = {header: value for header, value in zip(headers, row)}
            batch.append(row_dict)
            row_count += 1
            
            if len(batch) >= BATCH_SIZE:
                if self.rest_client.insert_batch("lobby_staging", batch):
                    print(f"  Streamed {row_count:,} rows...")
                else:
                    print(f"✗ Failed to insert batch at row {row_count}")
                    sys.exit(1)
                batch = []
        
        # Insert final batch
        if batch:
            if self.rest_client.insert_batch("lobby_staging", batch):
                print(f"✓ Successfully streamed {row_count:,} rows using REST API")
            else:
                print(f"✗ Failed to insert final batch")
                sys.exit(1)
    
    def close(self) -> None:
        """Close the database connection."""
        if self.postgres_conn:
            self.postgres_conn.close()


class SupabaseRESTClient:
    """Simple REST API client for Supabase."""
    
    def __init__(self, url: str, service_key: str):
        self.url = url.rstrip('/')
        self.service_key = service_key
        self.headers = {
            'apikey': service_key,
            'Authorization': f'Bearer {service_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        self.rest_url = f"{self.url}/rest/v1"
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            response = requests.get(
                f"{self.rest_url}/{table_name}?limit=1",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def insert_batch(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Insert a batch of records."""
        try:
            response = requests.post(
                f"{self.rest_url}/{table_name}",
                headers=self.headers,
                json=data,
                timeout=60
            )
            return response.status_code in [200, 201, 204]
        except Exception as e:
            print(f"Batch insert failed: {e}")
            return False


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            os.unlink(file_path)
        except OSError:
            pass


def main() -> None:
    """Main ETL process with automatic connection fallback."""
    print("🏛️  Canadian Lobbying Data ETL (Enhanced)")
    print("=" * 55)
    
    load_env_file()
    start_time = datetime.now()
    zip_path = None
    csv_path = None
    
    try:
        # Download and extract data
        zip_path = download_lobbying_data()
        csv_path, headers = extract_primary_csv(zip_path)
        
        # Establish database connection with fallback
        conn_manager = ConnectionManager()
        method = conn_manager.establish_connection()
        
        print(f"✓ Using connection method: {method.value}")
        
        # Create table and insert data
        conn_manager.create_table(headers)
        conn_manager.insert_data(csv_path, headers)
        
        # Success summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 55)
        print(f"✓ ETL completed successfully in {duration}")
        print(f"✓ Data loaded into lobby_staging table via {method.value}")
        print(f"✓ Columns: {len(headers)}")
        
        conn_manager.close()
        
    except KeyboardInterrupt:
        print("\n✗ ETL interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ETL failed: {e}")
        sys.exit(1)
    finally:
        cleanup_temp_files(zip_path, csv_path)


if __name__ == "__main__":
    main()