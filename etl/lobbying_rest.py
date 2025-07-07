#!/usr/bin/env python3
"""
ETL script for Canadian lobbying registrations data using Supabase REST API.
Downloads, processes, and loads data into Supabase via REST API.
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
from typing import Iterator, Dict, Any, List
from urllib.request import urlretrieve
from urllib.error import URLError
from pathlib import Path

import requests


# Data source URL from Commissioner of Lobbying Canada
LOBBYING_DATA_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"

# Date threshold for filtering (last 2 years)
CUTOFF_DATE = datetime.now() - timedelta(days=730)

# Batch size for REST API insertions
BATCH_SIZE = 1000


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
    # Remove special characters and replace with underscores
    name = re.sub(r'[^\w\s]', '_', name)
    # Replace spaces with underscores
    name = re.sub(r'\s+', '_', name)
    # Convert to lowercase
    name = name.lower()
    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
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
        # Look for the primary registrations CSV (usually the largest file)
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        
        if not csv_files:
            print("✗ No CSV files found in archive")
            sys.exit(1)
        
        # Assume the first/largest CSV is the primary registrations file
        primary_csv = csv_files[0]
        print(f"✓ Found primary CSV: {primary_csv}")
        
        # Extract to temporary file
        temp_csv = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
        temp_csv.close()
        
        with zip_ref.open(primary_csv) as source:
            with open(temp_csv.name, 'wb') as target:
                target.write(source.read())
        
        # Read headers and convert to snake_case
        # Try different encodings to handle the CSV file
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
    
    # Try to find a date column (common names)
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
                            # Try multiple date formats
                            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                                try:
                                    row_date = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                # If no format matches, include the row
                                yield row
                                filtered_count += 1
                                continue
                            
                            if row_date >= CUTOFF_DATE:
                                yield row
                                filtered_count += 1
                                
                        except (ValueError, IndexError):
                            # If date parsing fails, include the row
                            yield row
                            filtered_count += 1
            
            print(f"✓ Filtered {filtered_count:,} rows from {total_count:,} total rows")
            return
            
        except UnicodeDecodeError:
            continue
    
    print("✗ Could not read CSV file with any encoding")
    sys.exit(1)


class SupabaseClient:
    """Supabase REST API client for database operations."""
    
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
    
    def test_connection(self) -> bool:
        """Test the connection to Supabase."""
        try:
            response = requests.get(f"{self.rest_url}/", headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists by trying to query it."""
        try:
            response = requests.get(
                f"{self.rest_url}/{table_name}?limit=1",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def clear_table(self, table_name: str) -> bool:
        """Clear all data from a table."""
        try:
            # Delete all records (Supabase REST API doesn't support DELETE without WHERE)
            # So we'll just note that the table should be cleared manually if needed
            return True
        except Exception as e:
            print(f"Clear table failed: {e}")
            return False
    
    def insert_batch(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Insert a batch of records into the table."""
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


def setup_staging_table(client: SupabaseClient, headers: list[str]) -> None:
    """Check if lobby_staging table exists, create it if needed."""
    print("Setting up lobby_staging table...")
    
    if client.table_exists("lobby_staging"):
        print("✓ lobby_staging table already exists")
        return
    
    # Table doesn't exist - provide instructions to create it
    print("✗ lobby_staging table doesn't exist")
    print("\nTo create the table, run this SQL in your Supabase SQL editor:")
    print("=" * 60)
    
    columns_sql = ",\n    ".join([f"{col} TEXT" for col in headers])
    create_sql = f"""
CREATE TABLE lobby_staging (
    {columns_sql}
);
"""
    print(create_sql)
    print("=" * 60)
    print("\nAfter creating the table, run this script again.")
    sys.exit(1)


def stream_to_supabase_rest(csv_path: str, headers: list[str]) -> None:
    """Stream the filtered CSV data to Supabase using REST API."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
    
    if not supabase_url or not supabase_key:
        print("✗ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables")
        sys.exit(1)
    
    print("Connecting to Supabase via REST API...")
    
    # Initialize client
    client = SupabaseClient(supabase_url, supabase_key)
    
    # Test connection
    if not client.test_connection():
        print("✗ Failed to connect to Supabase REST API")
        sys.exit(1)
    
    print("✓ Connected to Supabase REST API")
    
    # Setup staging table
    setup_staging_table(client, headers)
    
    # Stream data in batches
    print("Streaming data to Supabase...")
    
    batch = []
    row_count = 0
    
    try:
        for row in filter_recent_rows(csv_path, headers):
            # Convert row to dictionary
            row_dict = {header: value for header, value in zip(headers, row)}
            batch.append(row_dict)
            row_count += 1
            
            # Insert batch when full
            if len(batch) >= BATCH_SIZE:
                if client.insert_batch("lobby_staging", batch):
                    print(f"  Streamed {row_count:,} rows...")
                else:
                    print(f"✗ Failed to insert batch at row {row_count}")
                    sys.exit(1)
                batch = []
        
        # Insert final batch
        if batch:
            if client.insert_batch("lobby_staging", batch):
                print(f"✓ Successfully streamed {row_count:,} rows to lobby_staging table")
            else:
                print(f"✗ Failed to insert final batch")
                sys.exit(1)
                
    except Exception as e:
        print(f"✗ Streaming error: {e}")
        sys.exit(1)


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            os.unlink(file_path)
        except OSError:
            pass


def main() -> None:
    """Main ETL process using REST API."""
    print("🏛️  Canadian Lobbying Data ETL (REST API)")
    print("=" * 50)
    
    # Load environment variables from .env file
    load_env_file()
    
    start_time = datetime.now()
    zip_path = None
    csv_path = None
    
    try:
        # Download data
        zip_path = download_lobbying_data()
        
        # Extract CSV
        csv_path, headers = extract_primary_csv(zip_path)
        
        # Stream to Supabase via REST API
        stream_to_supabase_rest(csv_path, headers)
        
        # Success summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 50)
        print(f"✓ ETL completed successfully in {duration}")
        print(f"✓ Data loaded into lobby_staging table via REST API")
        print(f"✓ Columns: {len(headers)}")
        
    except KeyboardInterrupt:
        print("\n✗ ETL interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ETL failed: {e}")
        sys.exit(1)
    finally:
        # Clean up temporary files
        cleanup_temp_files(zip_path, csv_path)


if __name__ == "__main__":
    main()