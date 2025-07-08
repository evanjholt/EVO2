#!/usr/bin/env python3
"""
Local ETL script for Canadian lobbying registrations data.
Connects to local Supabase PostgreSQL (localhost:54322) for development.
"""

import os
import sys
import re
import csv
import zipfile
import tempfile
from datetime import datetime, timedelta
from typing import Iterator
from urllib.request import urlretrieve
from urllib.error import URLError

import psycopg
from psycopg.sql import SQL, Identifier


# Configuration constants
LOBBYING_DATA_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"
DAYS_BACK = 730  # Filter data from last 2 years
BATCH_SIZE = 1000  # Batch size for database inserts

# Date threshold for filtering
CUTOFF_DATE = datetime.now() - timedelta(days=DAYS_BACK)


def snake_case(name: str) -> str:
    """Convert column name to snake_case."""
    name = re.sub(r'[^\w\s]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = name.lower()
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def detect_file_encoding(file_path: str) -> str:
    """Detect the encoding of a CSV file."""
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)  # Read a small chunk to test encoding
                return encoding
        except UnicodeDecodeError:
            continue
    
    raise ValueError("Could not determine file encoding")


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
    """Extract the Registration_Primary Export CSV and return headers."""
    print("Extracting Registration_Primary Export CSV from ZIP archive...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        
        if not csv_files:
            print("✗ No CSV files found in archive")
            sys.exit(1)
        
        # Look for Registration_Primary Export file specifically
        primary_csv = None
        for csv_file in csv_files:
            if 'Registration_Primary' in csv_file or 'registration_primary' in csv_file.lower():
                primary_csv = csv_file
                break
        
        if not primary_csv:
            print("Available CSV files:")
            for f in csv_files:
                print(f"  - {f}")
            print("✗ Registration_Primary Export file not found in archive")
            sys.exit(1)
        
        print(f"✓ Found Registration_Primary Export: {primary_csv}")
        
        temp_csv = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
        temp_csv.close()
        
        with zip_ref.open(primary_csv) as source:
            with open(temp_csv.name, 'wb') as target:
                target.write(source.read())
        
        # Define the specific columns we want to extract
        target_columns = [
            'REG_ID_ENR',
            'REG_TYPE_ENR', 
            'EFFECTIVE_DATE_VIGUEUR',
            'END_DATE_FIN',
            'EN_FIRM_NM_FIRME_AN',
            'CLIENT_ORG_CORP_NUM',
            'EN_CLIENT_ORG_CORP_NM_AN',
            'SUBSIDIARY_IND_FILIALE',
            'PARENT_IND_SOC_MERE',
            'RGSTRNT_1ST_NM_PRENOM_DCLRNT',
            'RGSTRNT_LAST_NM_DCLRNT',
            'RGSTRNT_ADDRESS_ADRESSE_DCLRNT',
            'GOVT_FUND_IND_FIN_GOUV',
            'FY_END_DATE_FIN_EXERCICE',
            'POSTED_DATE_PUBLICATION'
        ]
        
        # Read headers with encoding detection
        try:
            encoding = detect_file_encoding(temp_csv.name)
            print(f"✓ Using encoding: {encoding}")
            
            with open(temp_csv.name, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Find column indices for our target columns
                column_indices = []
                snake_headers = []
                
                for target_col in target_columns:
                    try:
                        idx = headers.index(target_col)
                        column_indices.append(idx)
                        snake_headers.append(snake_case(target_col))
                    except ValueError:
                        print(f"✗ Column '{target_col}' not found in CSV")
                        print(f"Available columns: {', '.join(headers[:10])}...")  # Show first 10
                        sys.exit(1)
                        
        except ValueError as e:
            print(f"✗ {e}")
            sys.exit(1)
        
        print(f"✓ Extracted CSV with {len(snake_headers)} target columns")
        return temp_csv.name, snake_headers, column_indices


def filter_recent_rows(csv_path: str, headers: list[str], column_indices: list[int]) -> Iterator[list[str]]:
    """Filter CSV rows to only include registrations from the last 2 years."""
    print(f"Filtering data to last 2 years (since {CUTOFF_DATE.strftime('%Y-%m-%d')})...")
    
    # Find the index for POSTED_DATE_PUBLICATION in snake_case headers
    posted_date_idx = None
    for i, header in enumerate(headers):
        if header == 'posted_date_publication':
            posted_date_idx = i
            break
    
    if posted_date_idx is None:
        print("✗ POSTED_DATE_PUBLICATION column not found in target headers")
        sys.exit(1)
    
    # Read CSV file with detected encoding
    try:
        encoding = detect_file_encoding(csv_path)
        filtered_count = 0
        total_count = 0
        
        with open(csv_path, 'r', encoding=encoding) as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            
            for row in reader:
                total_count += 1
                
                # Extract target columns
                filtered_row = []
                for idx in column_indices:
                    if idx < len(row):
                        filtered_row.append(row[idx])
                    else:
                        filtered_row.append('')  # Empty string for missing columns
                
                # Check POSTED_DATE_PUBLICATION for filtering
                if posted_date_idx < len(filtered_row):
                    date_str = filtered_row[posted_date_idx]
                    
                    # Skip rows with null or empty dates
                    if date_str in ['null', '', None]:
                        continue
                        
                    try:
                        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                row_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            # If we can't parse the date, skip the row
                            continue
                        
                        if row_date >= CUTOFF_DATE:
                            yield filtered_row
                            filtered_count += 1
                            
                    except (ValueError, IndexError):
                        # If date parsing fails, skip the row
                        continue
        
        print(f"✓ Filtered {filtered_count:,} rows from {total_count:,} total rows")
        
    except ValueError as e:
        print(f"✗ {e}")
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
        """Create the Registration_PrimaryExport table."""
        print("Creating Registration_PrimaryExport table...")
        
        columns = [f"{header} TEXT" for header in headers]
        columns_sql = ",\n    ".join(columns)
        
        create_table_sql = f"""
        DROP TABLE IF EXISTS "Registration_PrimaryExport";
        CREATE TABLE "Registration_PrimaryExport" (
            {columns_sql}
        );
        CREATE INDEX IF NOT EXISTS "idx_Registration_PrimaryExport_reg_id" ON "Registration_PrimaryExport"(reg_id_enr);
        CREATE INDEX IF NOT EXISTS "idx_Registration_PrimaryExport_posted_date" ON "Registration_PrimaryExport"(posted_date_publication);
        """
        
        with self.postgres_conn.cursor() as cur:
            cur.execute(create_table_sql)
            self.postgres_conn.commit()
        
        print("✓ Created Registration_PrimaryExport table with indexes")
    
    def insert_data(self, csv_path: str, headers: list[str], column_indices: list[int]) -> None:
        """Insert data using PostgreSQL executemany."""
        print("Inserting data using PostgreSQL executemany...")
        
        with self.postgres_conn.cursor() as cur:
            # Create the SQL statement
            insert_sql = SQL("INSERT INTO {} ({}) VALUES ({})").format(
                Identifier('Registration_PrimaryExport'),
                SQL(', ').join(map(Identifier, headers)),
                SQL(', ').join(SQL('%s') for _ in headers)
            )
            
            # Collect all rows first
            all_rows = list(filter_recent_rows(csv_path, headers, column_indices))
            row_count = len(all_rows)
            
            if row_count > 0:
                # Insert data in batches
                for i in range(0, row_count, BATCH_SIZE):
                    batch = all_rows[i:i + BATCH_SIZE]
                    cur.executemany(insert_sql, batch)
                    
                    if (i + BATCH_SIZE) % 10000 == 0:
                        print(f"  Inserted {i + BATCH_SIZE:,} rows...")
                
                self.postgres_conn.commit()
                print(f"✓ Successfully inserted {row_count:,} rows using PostgreSQL executemany")
            else:
                print("⚠ No rows to insert after filtering")
    
    def close(self) -> None:
        """Close the database connection."""
        if self.postgres_conn:
            self.postgres_conn.close()


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        if file_path:  # Only try to delete if path exists
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
        csv_path, headers, column_indices = extract_primary_csv(zip_path)
        
        # Establish local database connection
        conn_manager = LocalConnectionManager()
        conn_manager.establish_connection()
        
        # Create table and insert data
        conn_manager.create_table(headers)
        conn_manager.insert_data(csv_path, headers, column_indices)
        
        # Success summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 50)
        print(f"✓ ETL completed successfully in {duration}")
        print(f"✓ Data loaded into Registration_PrimaryExport table via local PostgreSQL")
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