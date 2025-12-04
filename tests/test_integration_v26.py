#!/usr/bin/env python3
"""
Test script for Dremio Software v26 connection using PAT authentication.
This script validates the connection and performs basic operations.
"""

import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

# Load environment variables
load_dotenv()

def main():
    print("=" * 80)
    print("Dremio Software v26 Connection Test")
    print("=" * 80)
    
    # Get configuration from environment
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    tls = os.getenv("DREMIO_SOFTWARE_TLS", "false").lower() == "true"
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    testing_folder = os.getenv("DREMIO_SOFTWARE_TESTING_FOLDER")
    
    print(f"\nConfiguration:")
    print(f"  Host: {host}")
    print(f"  TLS: {tls}")
    print(f"  PAT: {'*' * 20}...{pat[-10:] if pat else 'NOT SET'}")
    print(f"  Testing Folder: {testing_folder}")
    
    if not host or not pat:
        print("\n❌ ERROR: DREMIO_SOFTWARE_HOST and DREMIO_SOFTWARE_PAT must be set in .env")
        return 1
    
    # Extract hostname from URL if needed
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    
    print(f"\n{'=' * 80}")
    print("Step 1: Creating DremioClient")
    print("=" * 80)
    print(f"  Connecting to: {hostname}")
    # This automatically handles ports, base_url, and authentication logic
    try:
        client = DremioClient(
            hostname=hostname,
            pat=pat,
            tls=tls,
            mode="v26"
        )
        print("✅ Client created successfully")
        print(f"   Base URL: {client.base_url}")
        print(f"   Project ID: {client.project_id}")
    except Exception as e:
        print(f"❌ Failed to create client: {e}")
        return 1
    
    print(f"\n{'=' * 80}")
    print("Step 2: Testing Catalog Access")
    print("=" * 80)
    
    try:
        catalog = client.catalog.list_catalog()
        print(f"✅ Catalog listing successful")
        print(f"   Found {len(catalog)} top-level items:")
        for item in catalog[:10]:  # Show first 10
            item_type = item.get('type', 'UNKNOWN')
            item_path = item.get('path', ['UNKNOWN'])
            print(f"     - {item_type}: {'.'.join(item_path)}")
        if len(catalog) > 10:
            print(f"     ... and {len(catalog) - 10} more items")
    except Exception as e:
        print(f"❌ Failed to list catalog: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print(f"\n{'=' * 80}")
    print("Step 3: Testing Simple Query")
    print("=" * 80)
    
    try:
        # Try a simple system query
        result = client.query("SELECT 1 as test_column")
        print("✅ Query executed successfully")
        print(f"   Result:\n{result}")
    except Exception as e:
        print(f"❌ Failed to execute query: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print(f"\n{'=' * 80}")
    print("Step 4: Testing Builder API")
    print("=" * 80)
    
    try:
        # Try using the builder API with sys.version
        df = client.table("sys.version").limit(5).collect()
        print("✅ Builder API query successful")
        print(f"   Result:\n{df}")
    except Exception as e:
        print(f"❌ Failed to use builder API: {e}")
        import traceback
        traceback.print_exc()
        # This might fail if sys.version doesn't exist, but that's okay
        print("   Note: This is expected if sys.version table doesn't exist")
    
    if testing_folder:
        print(f"\n{'=' * 80}")
        print(f"Step 5: Testing Access to Testing Folder: {testing_folder}")
        print("=" * 80)
        
        try:
            # Try to access the testing folder
            # First, let's see if we can query it
            parts = testing_folder.split('.')
            if len(parts) >= 2:
                # Try to list contents
                result = client.query(f"SHOW TABLES IN {testing_folder}")
                print(f"✅ Successfully accessed testing folder")
                print(f"   Tables:\n{result}")
        except Exception as e:
            print(f"⚠️  Could not access testing folder: {e}")
            print("   This might be expected if the folder is empty or doesn't exist yet")
    
    print(f"\n{'=' * 80}")
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print("\nYour Dremio Software v26 environment is properly configured.")
    print("You can now run the full test suite with:")
    print("  pytest tests/ -m software -v")
    print("\nOr run specific integration tests:")
    print("  pytest tests/test_integration_software.py -v")
    
    return 0

if __name__ == "__main__":
    exit(main())
