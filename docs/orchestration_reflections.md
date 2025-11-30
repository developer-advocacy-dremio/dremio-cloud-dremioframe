# Reflection Management

`dremioframe` simplifies managing Dremio Reflections.

## RefreshReflectionTask

Triggers a refresh of all reflections on a specific dataset.

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
