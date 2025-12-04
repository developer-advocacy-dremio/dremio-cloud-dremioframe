#!/usr/bin/env python3
"""
Simple REST API test for Dremio Software v26 connection.
This tests only the catalog/REST API functionality, not Arrow Flight queries.
"""

import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

# Load environment variables
load_dotenv()

def main():
    print("=" * 80)
    print("Dremio Software v26 REST API Connection Test")
    print("=" * 80)
    
    # Get configuration from environment
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    tls = os.getenv("DREMIO_SOFTWARE_TLS", "false").lower() == "true"
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    print(f"\nConfiguration:")
    print(f"  Host: {host}")
    print(f"  TLS: {tls}")
    print(f"  PAT: {'*' * 20}...{pat[-10:] if pat else 'NOT SET'}")
    
    if not host or not pat:
        print("\n❌ ERROR: DREMIO_SOFTWARE_HOST and DREMIO_SOFTWARE_PAT must be set in .env")
        return 1
    
    # Extract hostname from URL
    original_url = host
    hostname = host.replace("https://", "").replace("http://", "")
    
    # Determine port from URL
    if ":" in hostname:
        hostname, port_str = hostname.split(":", 1)
        port = int(port_str)
    elif original_url.startswith("https://"):
        port = 443
    elif original_url.startswith("http://"):
        port = 9047
    else:
        port = 9047
    
    print(f"\n{'=' * 80}")
    print("Creating DremioClient")
    print("=" * 80)
    print(f"  Connecting to: {hostname}:{port}")
    
    # Unset Cloud-specific env vars
    if "DREMIO_PROJECT_ID" in os.environ:
        print("  Note: Temporarily unsetting DREMIO_PROJECT_ID for Software connection")
        del os.environ["DREMIO_PROJECT_ID"]
    if "DREMIO_PAT" in os.environ:
        print("  Note: Temporarily unsetting DREMIO_PAT (using DREMIO_SOFTWARE_PAT instead)")
        del os.environ["DREMIO_PAT"]
    
    # Construct the base_url
    protocol = "https" if tls else "http"
    base_url = f"{protocol}://{hostname}:{port}/api/v3"
    
    try:
        client = DremioClient(
            hostname=hostname,
            port=port,
            pat=pat,
            project_id=None,
            base_url=base_url,
            tls=tls,
            disable_certificate_verification=False
        )
        print("✅ Client created successfully")
        print(f"   Base URL: {client.base_url}")
    except Exception as e:
        print(f"❌ Failed to create client: {e}")
        return 1
    
    print(f"\n{'=' * 80}")
    print("Testing Catalog Access")
    print("=" * 80)
    
    try:
        catalog = client.catalog.list_catalog()
        print(f"✅ Catalog listing successful!")
        print(f"   Found {len(catalog)} top-level items:")
        for item in catalog[:10]:
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
    print("✅ REST API CONNECTION SUCCESSFUL!")
    print("=" * 80)
    print("\nYour Dremio Software v26 REST API connection is working correctly.")
    print("\nNote: Arrow Flight queries require additional configuration.")
    print("To enable Flight queries, you need to set DREMIO_SOFTWARE_FLIGHT_PORT")
    print("in your .env file (typically 32010 for Dremio Software).")
    print("\nExample .env addition:")
    print("  DREMIO_SOFTWARE_FLIGHT_PORT=32010")
    
    return 0

if __name__ == "__main__":
    exit(main())
