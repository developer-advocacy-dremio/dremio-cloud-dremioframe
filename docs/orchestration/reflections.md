# Reflection Management

`dremioframe` simplifies managing Dremio Reflections.

## RefreshReflectionTask

Triggers a refresh of all reflections on a specific dataset.

### Arguments
-   `name` (str): The unique name of the task.
-   `client` (DremioClient): The authenticated Dremio client.
-   `dataset` (str): The full path to the dataset (e.g., `source.folder.dataset`).

### Example
```python
from dremioframe.orchestration import RefreshReflectionTask

t_refresh = RefreshReflectionTask(
    name="refresh_sales_reflections",
    client=client,
    dataset="my_catalog.sales"
)
```

This task executes:
```sql
ALTER DATASET my_catalog.sales REFRESH REFLECTIONS
```
