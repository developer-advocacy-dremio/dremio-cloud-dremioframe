# Catalog & Admin

The `Catalog` class provides access to Dremio's catalog and administrative functions via the REST API.

## Accessing the Catalog

You can access the catalog through the `DremioClient` instance:

```python
from dremioframe.client import DremioClient

client = DremioClient()
catalog = client.catalog
```

## Listing Items

To list the contents of the root catalog or a specific path:

```python
# List root catalog
items = catalog.list_catalog()
for item in items:
    print(item['path'], item['type'])

# List contents of a source or folder
items = catalog.list_catalog("Samples")
```

## Managing Sources

You can create and delete sources:

```python
# Create a source (example for S3)
config = {
    "bucketName": "my-bucket",
    "authenticationType": "ACCESS_KEY",
    "accessKey": "...",
    "accessSecret": "..."
}
catalog.create_source("MyS3Source", "S3", config)

# Delete a source
catalog.delete_catalog_item("source-id-uuid")
```

## Managing Views

You can also manage views (virtual datasets) if you know their ID or path.

```python
# Get entity details
view = catalog.get_entity("MySpace.MyView")
print(view)
```
