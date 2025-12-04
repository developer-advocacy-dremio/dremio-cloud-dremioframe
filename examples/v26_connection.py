#!/usr/bin/env python3
"""
Simple example: Connecting to Dremio Software v26 with DremioFrame

This demonstrates the new, simplified connection approach.
"""

from dremioframe.client import DremioClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# That's it! Just specify mode='v26' and the client handles everything:
# - Automatically uses port 443 for REST API (with TLS)
# - Automatically uses port 32010 for Arrow Flight
# - Automatically sets project_id to None (Software mode)
# - Reads DREMIO_SOFTWARE_PAT from environment

client = DremioClient(
    hostname="v26.dremio.org",
    pat=os.getenv("DREMIO_SOFTWARE_PAT"),
    tls=True,
    mode="v26"  # This is the magic! No more manual port configuration
)

# Or even simpler - let it auto-detect from environment variables:
# client = DremioClient(mode="v26")

# Now use the client normally
print("Listing catalog items...")
catalog = client.catalog.list_catalog()

print(f"\nFound {len(catalog)} top-level catalog items:")
for item in catalog[:5]:
    item_type = item.get('type', 'UNKNOWN')
    item_path = '.'.join(item.get('path', ['UNKNOWN']))
    print(f"  - {item_type}: {item_path}")

if len(catalog) > 5:
    print(f"  ... and {len(catalog) - 5} more")

print("\n✅ Connection successful!")
