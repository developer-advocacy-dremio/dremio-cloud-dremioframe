#!/usr/bin/env python3
"""
Test the new mode-based DremioClient initialization for v26
"""

import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

def main():
    print("=" * 80)
    print("Testing DremioClient with mode='v26'")
    print("=" * 80)
    
    # Get configuration
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    # Extract hostname
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    
    print(f"\nCreating client with mode='v26'...")
    print(f"  Hostname: {hostname}")
    
    # Create client with mode='v26' - should "just work"!
    client = DremioClient(
        hostname=hostname,
        pat=pat,
        tls=True,
        mode="v26"
    )
    
    print(f"\n✅ Client created successfully!")
    print(f"  Mode: {client.mode}")
    print(f"  Base URL: {client.base_url}")
    print(f"  REST Port: {client.port}")
    print(f"  Flight Port: {client.flight_port}")
    print(f"  Flight Endpoint: {client.flight_endpoint}")
    print(f"  Project ID: {client.project_id}")
    
    # Test catalog access
    print(f"\nTesting catalog access...")
    catalog = client.catalog.list_catalog()
    print(f"✅ Found {len(catalog)} catalog items")
    
    print(f"\n{'=' * 80}")
    print("SUCCESS! The mode='v26' configuration works perfectly.")
    print("=" * 80)
    
    return 0

if __name__ == "__main__":
    exit(main())
