#!/usr/bin/env python3
"""
Simple test to verify Cloud connection works with default mode
"""

from dremioframe.client import DremioClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("Testing Dremio Cloud connection with default mode...")
print("=" * 80)

try:
    # Default mode should be 'cloud' and pick up DREMIO_PAT and DREMIO_PROJECT_ID
    client = DremioClient()
    
    print(f"✅ Client created successfully")
    print(f"  Mode: {client.mode}")
    print(f"  Project ID: {client.project_id}")
    
    # Test catalog access
    print("\nTesting catalog access...")
    catalog = client.catalog.list_catalog()
    print(f"✅ Catalog access successful! Found {len(catalog)} items")
    
    for item in catalog[:5]:
        item_type = item.get('type', 'UNKNOWN')
        item_path = '.'.join(item.get('path', ['UNKNOWN']))
        print(f"  - {item_type}: {item_path}")
    
    print("\n" + "=" * 80)
    print("✅ Cloud connection test PASSED!")
    print("=" * 80)
    
except Exception as e:
    print(f"\n❌ Cloud connection test FAILED: {e}")
    import traceback
    traceback.print_exc()
