# Dataset Tagging

DremioFrame allows you to manage tags for datasets (tables and views) using the `Catalog` client. Tags are useful for organizing and classifying data assets (e.g., "PII", "Gold", "Deprecated").

## Usage

Access tagging methods via `client.catalog`.

### Get Tags

Retrieve the current tags for a dataset using its ID.

```python
from dremioframe.client import DremioClient

client = DremioClient()
dataset_id = "..." # Get ID via client.catalog.get_entity(...)

tags = client.catalog.get_tags(dataset_id)
print(tags) # e.g. ["pii", "sales"]
```

### Set Tags

Set the tags for a dataset. **Note:** This overwrites existing tags.

```python
# To safely update tags, it's recommended to fetch the current version first
# to avoid conflicts if tags were modified concurrently.

# 1. Get current tag info (includes version)
tag_info = client.catalog.get_tag_info(dataset_id)
current_version = tag_info.get("version")
current_tags = tag_info.get("tags", [])

# 2. Modify tags
new_tags = current_tags + ["new-tag"]

# 3. Set tags with version
client.catalog.set_tags(dataset_id, new_tags, version=current_version)
```

### Remove Tags

To remove all tags, pass an empty list.

```python
client.catalog.set_tags(dataset_id, [], version=current_version)
```
