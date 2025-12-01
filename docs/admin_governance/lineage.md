# Data Lineage

DremioFrame provides access to the data lineage graph for datasets, allowing you to understand dependencies and data flow.

## Usage

Access lineage via `client.catalog.get_lineage(id)`.

```python
from dremioframe.client import DremioClient

client = DremioClient()
dataset_id = "..." 

# Get lineage graph
lineage = client.catalog.get_lineage(dataset_id)

# Inspect parents (upstream dependencies)
parents = lineage.get("parents", [])
for parent in parents:
    print(f"Parent: {parent['path']} ({parent['id']})")

# Inspect children (downstream dependencies)
children = lineage.get("children", [])
for child in children:
    print(f"Child: {child['path']} ({child['id']})")
```
