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

You can create and update virtual datasets (views). The `sql` argument accepts either a raw SQL string or a `DremioBuilder` object (DataFrame).

```python
# Create a view using SQL string
catalog.create_view(
    path=["Space", "MyView"],
    sql="SELECT * FROM source.table"
)

# Create a view using a DataFrame (Builder)
df = client.table("source.table").filter("id > 100")
catalog.create_view(
    path=["Space", "FilteredView"],
    sql=df
)
```

# Update a view (fetches latest version tag automatically)
```python
catalog.update_view(
    id="view-id-uuid",
    path=["Space", "MyView"],
    sql="SELECT * FROM source.table WHERE id > 100"
)
```

## Collaboration (Wikis & Tags)

Manage documentation and tags for any catalog entity (dataset, source, space, folder).

### Wikis

```python
# Get Wiki
wiki = catalog.get_wiki("entity-id")
print(wiki.get("text"))

# Update Wiki (fetch version first to avoid conflict)
try:
    current_wiki = catalog.get_wiki("entity-id")
    version = current_wiki.get("version")
except:
    version = None

catalog.update_wiki(
    id="entity-id",
    content="# My Dataset\n\nThis is a documented dataset.",
    version=version
)
```

### Tags

```python
# Get Tags
tags = catalog.get_tags("entity-id")
print(tags)

# Set Tags (Overwrites existing tags)
catalog.set_tags("entity-id", ["production", "marketing"])
```
