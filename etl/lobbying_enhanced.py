#!/usr/bin/env python3
"""
Local ETL script for Canadian lobbying registrations data.
Connects to local Supabase PostgreSQL (localhost:54322) for development.
"""

import os
import sys
import re
import io
import csv
import zipfile
import tempfile
from datetime import datetime, timedelta
from typing import Iterator
from urllib.request import urlretrieve
from urllib.error import URLError
from pathlib import Path

import psycopg


# Data source URL from Commissioner of Lobbying Canada
LOBBYING_DATA_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"

# Date threshold for filtering (last 2 years)
CUTOFF_DATE = datetime.now() - timedelta(days=730)


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


class LocalConnectionManager:
    """Manages local PostgreSQL connection for development."""
    
    def __init__(self):
        self.postgres_conn = None
    
    def establish_connection(self) -> None:
        """Establish local PostgreSQL connection."""
        print("Connecting to local Supabase PostgreSQL...")
        
        try:
            conn_string = "postgresql://postgres:postgres@localhost:54322/postgres"
            self.postgres_conn = psycopg.connect(conn_string, connect_timeout=10)
            print("✓ Local PostgreSQL connection successful")
        except Exception as e:
            print(f"✗ Local PostgreSQL connection failed: {e}")
            print("  Make sure Supabase is running: supabase start")
            sys.exit(1)
    
    def create_table(self, headers: list[str]) -> None:
        """Create the lobby_staging table."""
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
    
    def insert_data(self, csv_path: str, headers: list[str]) -> None:
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
    
    def close(self) -> None:
        """Close the database connection."""
        if self.postgres_conn:
            self.postgres_conn.close()


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            os.unlink(file_path)
        except OSError:
            pass


def main() -> None:
    """Main ETL process for local development."""
    print("🏛️  Canadian Lobbying Data ETL (Local)")
    print("=" * 50)
    
    start_time = datetime.now()
    zip_path = None
    csv_path = None
    
    try:
        # Download and extract data
        zip_path = download_lobbying_data()
        csv_path, headers = extract_primary_csv(zip_path)
        
        # Establish local database connection
        conn_manager = LocalConnectionManager()
        conn_manager.establish_connection()
        
        # Create table and insert data
        conn_manager.create_table(headers)
        conn_manager.insert_data(csv_path, headers)
        
        # Success summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 50)
        print(f"✓ ETL completed successfully in {duration}")
        print(f"✓ Data loaded into lobby_staging table via local PostgreSQL")
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