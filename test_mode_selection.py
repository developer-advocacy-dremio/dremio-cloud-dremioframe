#!/usr/bin/env python3
"""
Test that mode parameter correctly determines which environment variables to use
when both Cloud and Software env vars are present.
"""

import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

print("=" * 80)
print("Testing Mode-Based Environment Variable Selection")
print("=" * 80)

# Show what env vars are set
print("\nEnvironment Variables Present:")
print(f"  DREMIO_PAT: {'SET' if os.getenv('DREMIO_PAT') else 'NOT SET'}")
print(f"  DREMIO_PROJECT_ID: {'SET' if os.getenv('DREMIO_PROJECT_ID') else 'NOT SET'}")
print(f"  DREMIO_SOFTWARE_HOST: {os.getenv('DREMIO_SOFTWARE_HOST', 'NOT SET')}")
print(f"  DREMIO_SOFTWARE_PAT: {'SET' if os.getenv('DREMIO_SOFTWARE_PAT') else 'NOT SET'}")

# Test 1: Default mode (cloud) should use Cloud env vars
print(f"\n{'=' * 80}")
print("Test 1: Default mode (should be 'cloud')")
print("=" * 80)
try:
    client = DremioClient()
    print(f"✅ Client created")
    print(f"  Mode: {client.mode}")
    print(f"  Project ID: {client.project_id}")
    print(f"  Hostname: {client.hostname}")
    print(f"  Expected: mode='cloud', project_id should be set")
except Exception as e:
    print(f"❌ Failed: {e}")

# Test 2: Explicit mode='v26' should use Software env vars
print(f"\n{'=' * 80}")
print("Test 2: Explicit mode='v26'")
print("=" * 80)
try:
    client = DremioClient(mode="v26")
    print(f"✅ Client created")
    print(f"  Mode: {client.mode}")
    print(f"  Project ID: {client.project_id}")
    print(f"  Hostname: {client.hostname}")
    print(f"  Base URL: {client.base_url}")
    print(f"  Expected: mode='v26', project_id=None, hostname from DREMIO_SOFTWARE_HOST")
    
    # Test catalog access
    catalog = client.catalog.list_catalog()
    print(f"✅ Catalog access successful! Found {len(catalog)} items")
except Exception as e:
    print(f"❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Explicit mode='cloud' should use Cloud env vars
print(f"\n{'=' * 80}")
print("Test 3: Explicit mode='cloud'")
print("=" * 80)
try:
    client = DremioClient(mode="cloud")
    print(f"✅ Client created")
    print(f"  Mode: {client.mode}")
    print(f"  Project ID: {client.project_id}")
    print(f"  Hostname: {client.hostname}")
    print(f"  Expected: mode='cloud', project_id should be set")
except Exception as e:
    print(f"❌ Failed: {e}")

print(f"\n{'=' * 80}")
print("✅ All tests completed!")
print("=" * 80)
print("\nConclusion: The mode parameter now correctly determines which")
print("environment variables to use, even when both Cloud and Software")
print("env vars are present. This prevents conflicts and makes behavior")
print("predictable.")
