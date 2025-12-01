import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

pat = os.environ.get("DREMIO_PAT")
project_id = os.environ.get("DREMIO_PROJECT_ID")
client = DremioClient(pat=pat, project_id=project_id)

print("Sources:")
try:
    sources = client.admin.list_sources()
    for s in sources.get('data', []):
        print(f" - {s['name']}")
except Exception as e:
    print(f"Error listing sources: {e}")

print("\nCatalog Root:")
try:
    # List root entities
    # Catalog API usually lists root if no path provided?
    # Or we can try to get 'target' directly
    try:
        target = client.catalog.get_entity("target")
        print(f"Found 'target': {target}")
    except Exception as e:
        print(f"'target' not found: {e}")
        
    # List all root containers
    # We don't have a direct list_root method in Catalog class exposed easily maybe?
    # Let's try listing sources again via catalog if possible
    pass
except Exception as e:
    print(f"Error in catalog: {e}")
