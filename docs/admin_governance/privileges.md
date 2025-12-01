# Grants and Privileges

DremioFrame allows you to manage access control lists (grants) for catalog entities (datasets, folders, sources).

## Usage

Access grants via `client.catalog`.

### Get Grants

Retrieve the current grants for an entity.

```python
from dremioframe.client import DremioClient

client = DremioClient()
entity_id = "..." 

grants_info = client.catalog.get_grants(entity_id)
print(grants_info)
# Output example:
# {
#   "grants": [
#     {"granteeType": "USER", "id": "...", "privileges": ["SELECT", "ALTER"]},
#     {"granteeType": "ROLE", "id": "...", "privileges": ["SELECT"]}
#   ],
#   "availablePrivileges": ["SELECT", "ALTER", "MANAGE_GRANTS", ...]
# }
```

### Set Grants

Update the grants for an entity. **Note:** This replaces the existing grants list.

```python
new_grants = [
    {
        "granteeType": "USER",
        "id": "user-uuid-...",
        "privileges": ["SELECT"]
    }
]

client.catalog.set_grants(entity_id, new_grants)
```
