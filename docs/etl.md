# Canadian Lobbying Data ETL

## Overview
ETL pipeline for downloading and processing Canadian lobbying registrations data from the Commissioner of Lobbying of Canada's open data portal.

## File Sizes & Performance

### Expected Data Sizes
- **ZIP Archive**: ~50-100 MB (compressed)
- **Primary CSV**: ~200-500 MB (uncompressed)
- **Filtered Data**: ~50-150 MB (last 2 years only)
- **Row Count**: ~50,000-150,000 registrations (filtered)

### Runtime Expectations
- **Download**: 30-90 seconds (depends on connection)
- **Extraction**: 5-15 seconds
- **Filtering**: 10-30 seconds
- **Database Upload**: 2-5 minutes
- **Total Runtime**: 3-7 minutes

## Setup

### Prerequisites
- Python 3.11+
- Active Supabase project
- Environment variables configured

### Installation
```bash
# Install dependencies
pip install psycopg[binary]

# Or with development dependencies
pip install -e ".[dev]"
```

### Environment Variables
```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-role-key"
```

## Usage

### Run ETL Pipeline
```bash
python etl/lobbying.py
```

### Expected Output
```
🏛️  Canadian Lobbying Data ETL
========================================
Downloading lobbying data...
✓ Downloaded to /tmp/tmpXXXXXX.zip
Extracting CSV from ZIP archive...
✓ Found primary CSV: registrations.csv
✓ Extracted CSV with 25 columns
Filtering data to last 2 years (since 2023-07-07)...
✓ Filtered 75,432 rows from 245,123 total rows
Creating Registration_PrimaryExport table...
✓ Created Registration_PrimaryExport table
Connecting to Supabase...
Streaming data to Supabase...
  Streamed 1,000 rows...
  Streamed 2,000 rows...
  ...
✓ Successfully streamed 75,432 rows to Registration_PrimaryExport table

========================================
✓ ETL completed successfully in 0:04:32
✓ Data loaded into Registration_PrimaryExport table
✓ Columns: 25
```

## Data Schema

The script automatically converts all column names to `snake_case` format and creates a staging table with TEXT columns for all fields. Common columns include:

- `registration_number`
- `client_name`
- `lobbyist_name`
- `registration_date`
- `termination_date`
- `subject_matter`
- `government_institution`

## Notes

- Data is filtered to include only registrations from the last 2 years
- All columns are stored as TEXT in the staging table for flexibility
- The script handles various date formats automatically
- Temporary files are cleaned up after processing
- Uses streaming upload to handle large datasets efficiently