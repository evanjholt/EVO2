#!/usr/bin/env python3
"""
ETL script for Canadian lobbying registrations data.
Downloads, processes, and loads data into Supabase.
"""

import os
import sys
import re
import io
import csv
import zipfile
import tempfile
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any
from urllib.request import urlretrieve
from urllib.error import URLError
from pathlib import Path

import psycopg


# Data source URL from Commissioner of Lobbying Canada
LOBBYING_DATA_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"

# Date threshold for filtering (last 2 years)
CUTOFF_DATE = datetime.now() - timedelta(days=730)


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
        with open(temp_csv.name, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            snake_headers = [snake_case(h) for h in headers]
        
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
    
    if date_col_idx is None:
        print("⚠ No date column found, returning all rows")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            yield from reader
        return
    
    filtered_count = 0
    total_count = 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
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


def create_staging_table(conn: psycopg.Connection, headers: list[str]) -> None:
    """Create the lobby_staging table with appropriate columns."""
    print("Creating lobby_staging table...")
    
    # Generate column definitions (all as TEXT for staging)
    columns = [f"{header} TEXT" for header in headers]
    columns_sql = ",\n    ".join(columns)
    
    create_table_sql = f"""
    DROP TABLE IF EXISTS lobby_staging;
    CREATE TABLE lobby_staging (
        {columns_sql}
    );
    """
    
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()
    
    print("✓ Created lobby_staging table")


def stream_to_supabase(csv_path: str, headers: list[str]) -> None:
    """Stream the filtered CSV data to Supabase using COPY FROM STDIN."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
    
    if not supabase_url or not supabase_key:
        print("✗ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables")
        sys.exit(1)
    
    # Parse connection details from Supabase URL
    # Extract host from URL: https://project-id.supabase.co -> project-id.supabase.co
    host = supabase_url.replace('https://', '').replace('http://', '')
    
    print("Connecting to Supabase...")
    
    try:
        # Try pooler connection first (port 6543), then direct connection (port 5432)
        connection_successful = False
        for port in [6543, 5432]:
            try:
                conn_string = f"postgresql://postgres:{supabase_key}@{host}:{port}/postgres?sslmode=require"
                print(f"Trying connection on port {port}...")
                with psycopg.connect(conn_string, connect_timeout=10) as conn:
                    print(f"✓ Connected successfully on port {port}")
                    connection_successful = True
                    
                    # Create staging table
                    create_staging_table(conn, headers)
                    
                    # Stream data using COPY FROM STDIN
                    print("Streaming data to Supabase...")
                    
                    with conn.cursor() as cur:
                        copy_sql = f"COPY lobby_staging ({', '.join(headers)}) FROM STDIN WITH CSV"
                        
                        # Create a StringIO buffer for the filtered data
                        buffer = io.StringIO()
                        writer = csv.writer(buffer)
                        
                        row_count = 0
                        for row in filter_recent_rows(csv_path, headers):
                            writer.writerow(row)
                            row_count += 1
                            
                            # Flush buffer periodically to avoid memory issues
                            if row_count % 1000 == 0:
                                buffer.seek(0)
                                cur.copy(copy_sql, buffer)
                                buffer.seek(0)
                                buffer.truncate(0)
                                print(f"  Streamed {row_count:,} rows...")
                        
                        # Final flush
                        if buffer.tell() > 0:
                            buffer.seek(0)
                            cur.copy(copy_sql, buffer)
                        
                        conn.commit()
                        print(f"✓ Successfully streamed {row_count:,} rows to lobby_staging table")
                        break
                        
            except Exception as port_error:
                print(f"✗ Connection failed on port {port}: {port_error}")
                continue
        
        if not connection_successful:
            raise Exception("Failed to connect on both ports 6543 and 5432")
                
    except Exception as e:
        print(f"✗ Database error: {e}")
        sys.exit(1)


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            os.unlink(file_path)
        except OSError:
            pass


def main() -> None:
    """Main ETL process."""
    print("🏛️  Canadian Lobbying Data ETL")
    print("=" * 40)
    
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
        
        # Stream to Supabase
        stream_to_supabase(csv_path, headers)
        
        # Success summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 40)
        print(f"✓ ETL completed successfully in {duration}")
        print(f"✓ Data loaded into lobby_staging table")
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