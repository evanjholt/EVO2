# EVO2

Canadian Lobbying Data ETL Pipeline with Supabase Integration

## Overview

ETL pipeline for processing Canadian lobbying registration data from the Commissioner of Lobbying Canada. Features multiple connection methods with automatic fallback for maximum reliability and performance.

## Features

- **Automatic Connection Fallback**: Local PostgreSQL → Remote PostgreSQL → REST API
- **High Performance**: PostgreSQL COPY for 10x faster bulk loads when available  
- **Network Resilient**: Works through firewalls and network restrictions
- **Schema Versioning**: Database migrations with Supabase CLI
- **Encoding Detection**: Handles various CSV encodings automatically

## Quick Start

### Requirements
- Python 3.11+
- Supabase account with project configured in `.env`

### Installation
```bash
# Install dependencies
pip install -e .

# Run ETL with automatic connection detection
python etl/lobbying_enhanced.py
```

### Connection Methods

```bash
# Automatic fallback (recommended)
python etl/lobbying_unified.py --method=auto

# Local development (requires: supabase start)
python etl/lobbying_unified.py --method=local

# Remote PostgreSQL (requires network access)
python etl/lobbying_unified.py --method=remote

# REST API (works through firewalls)  
python etl/lobbying_unified.py --method=rest
```

## Project Structure

```
etl/
├── lobbying_enhanced.py    # Enhanced ETL with automatic fallback
├── lobbying_unified.py     # Unified interface for all methods
├── lobbying_rest.py        # REST API-only implementation
└── lobbying.py            # Original PostgreSQL implementation

supabase/
├── config.toml            # Supabase CLI configuration
└── migrations/            # Database schema migrations
```

## Performance

| Method | Speed | Network Requirements | Use Case |
|--------|-------|---------------------|----------|
| Local PostgreSQL | 🟢 Fastest (COPY) | None | Local development |
| Remote PostgreSQL | 🟢 Fast (COPY) | Direct DB access | Production |
| REST API | 🟡 Good (batches) | HTTPS only | Firewalls/restricted networks |

## Configuration

Set up your `.env` file with Supabase credentials:

```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
```

## Local Development

```bash
# Start local Supabase stack
supabase start

# Run ETL with local PostgreSQL
python etl/lobbying_unified.py --method=local

# View data in Supabase Studio: http://localhost:54323
```

See [SUPABASE_SETUP.md](SUPABASE_SETUP.md) for detailed setup documentation.