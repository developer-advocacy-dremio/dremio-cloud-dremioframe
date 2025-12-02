# Batch Operations

DremioFrame provides a `BatchManager` to perform bulk operations on the Dremio catalog efficiently using parallel API requests.

## BatchManager

The `BatchManager` utilizes a thread pool to execute multiple API calls concurrently, significantly speeding up operations like creating many folders or deleting multiple datasets.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.batch import BatchManager

client = DremioClient()
# Initialize with 10 concurrent workers
manager = BatchManager(client, max_workers=10)
```

### Creating Folders

Create multiple folders at once.

```python
paths = [
    "space.folder1",
    "space.folder2",
    "space.folder3"
]

results = manager.create_folders(paths)

for path, result in results.items():
    if "error" in result:
        print(f"Failed to create {path}: {result['error']}")
    else:
        print(f"Created {path}")
```

### Deleting Items

Delete multiple items (datasets, folders, spaces) by their ID.

```python
ids_to_delete = ["id1", "id2", "id3"]

results = manager.delete_items(ids_to_delete)

for id, success in results.items():
    if success is True:
        print(f"Deleted {id}")
    else:
        print(f"Failed to delete {id}: {success['error']}")
```

## Performance Considerations

- **Rate Limits**: Be mindful of Dremio's API rate limits when using a high number of workers.
- **Error Handling**: Batch operations return a dictionary of results where individual failures are captured without stopping the entire batch. Always check the results for errors.
