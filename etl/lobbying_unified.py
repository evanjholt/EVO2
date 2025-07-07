#!/usr/bin/env python3
"""
Unified ETL script for Canadian lobbying registrations data.
Supports multiple connection methods with automatic fallback.

Usage:
  python lobbying_unified.py --method=auto     # Automatic fallback (recommended)
  python lobbying_unified.py --method=local    # Local Supabase only
  python lobbying_unified.py --method=remote   # Remote PostgreSQL only  
  python lobbying_unified.py --method=rest     # REST API only
  python lobbying_unified.py --method=enhanced # Same as auto
"""

import argparse
import os
import sys
from pathlib import Path

# Add the current directory to Python path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

def main():
    parser = argparse.ArgumentParser(
        description='Canadian Lobbying Data ETL with multiple connection methods',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Connection Methods:
  auto     - Automatic fallback: Local → Remote PostgreSQL → REST API (recommended)
  enhanced - Same as auto
  local    - Local Supabase PostgreSQL only (requires supabase start)
  remote   - Remote Supabase PostgreSQL only (requires network access)
  rest     - REST API only (works through firewalls)
  postgres - Legacy: tries remote PostgreSQL only

Examples:
  python lobbying_unified.py                    # Uses auto fallback
  python lobbying_unified.py --method=auto     # Uses auto fallback
  python lobbying_unified.py --method=local    # Local development only
  python lobbying_unified.py --method=rest     # REST API only
        """
    )
    parser.add_argument(
        '--method', 
        choices=['auto', 'enhanced', 'local', 'remote', 'rest', 'postgres'], 
        default='auto',
        help='Connection method (default: auto)'
    )
    
    args = parser.parse_args()
    
    # Map method choices to implementations
    if args.method in ['auto', 'enhanced']:
        print("Using enhanced ETL with automatic connection fallback...")
        try:
            from lobbying_enhanced import main as enhanced_main
            enhanced_main()
        except ImportError:
            print("✗ Enhanced module not found")
            sys.exit(1)
    
    elif args.method == 'local':
        print("Using local Supabase PostgreSQL connection...")
        try:
            # Import and modify the enhanced version to force local connection
            from lobbying_enhanced import ConnectionManager, main as enhanced_main
            # Override the connection establishment to only try local
            original_establish = ConnectionManager.establish_connection
            def force_local(self):
                if self.test_local_postgres():
                    from lobbying_enhanced import ConnectionMethod
                    self.connection_method = ConnectionMethod.LOCAL_POSTGRES
                    import psycopg
                    conn_string = f"postgresql://postgres:postgres@localhost:54322/postgres"
                    self.postgres_conn = psycopg.connect(conn_string, connect_timeout=10)
                    return ConnectionMethod.LOCAL_POSTGRES
                else:
                    print("✗ Local PostgreSQL connection failed. Is 'supabase start' running?")
                    sys.exit(1)
            ConnectionManager.establish_connection = force_local
            enhanced_main()
        except ImportError:
            print("✗ Enhanced module not found")
            sys.exit(1)
    
    elif args.method == 'remote':
        print("Using remote Supabase PostgreSQL connection...")
        try:
            from lobbying import main as postgres_main
            postgres_main()
        except ImportError:
            print("✗ PostgreSQL module not found")
            sys.exit(1)
    
    elif args.method == 'rest':
        print("Using REST API connection...")
        try:
            from lobbying_rest import main as rest_main
            rest_main()
        except ImportError:
            print("✗ REST API module not found")
            sys.exit(1)
    
    elif args.method == 'postgres':
        print("Using legacy PostgreSQL connection method...")
        try:
            from lobbying import main as postgres_main
            postgres_main()
        except ImportError:
            print("✗ PostgreSQL module not found")
            sys.exit(1)


if __name__ == "__main__":
    main()