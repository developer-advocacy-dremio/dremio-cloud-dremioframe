# Iceberg Maintenance Tasks

`dremioframe` simplifies Iceberg table maintenance with pre-built tasks.

## OptimizeTask

Runs `OPTIMIZE TABLE` to compact small files.

### Arguments
-   `name` (str): The unique name of the task.
-   `client` (DremioClient): The authenticated Dremio client.
-   `table` (str): The full path to the Iceberg table (e.g., `source.folder.table`).
-   `rewrite_data_files` (bool, default=True): Whether to include `REWRITE DATA USING BIN_PACK`.

### Example
```python
from dremioframe.orchestration import OptimizeTask

t_opt = OptimizeTask(
    name="optimize_sales",
    client=client,
    table="my_catalog.sales",
    rewrite_data_files=True
)
```

## VacuumTask

Runs `VACUUM TABLE` to remove unused files and expire snapshots.

### Arguments
-   `name` (str): The unique name of the task.
-   `client` (DremioClient): The authenticated Dremio client.
-   `table` (str): The full path to the Iceberg table.
-   `expire_snapshots` (bool, default=True): Whether to include `EXPIRE SNAPSHOTS`.
-   `retain_last` (int, optional): Number of recent snapshots to retain.
-   `older_than` (str, optional): Timestamp string (e.g., '2023-01-01 00:00:00') to expire snapshots older than.

### Example
```python
from dremioframe.orchestration import VacuumTask

t_vac = VacuumTask(
    name="vacuum_sales",
    client=client,
    table="my_catalog.sales",
    expire_snapshots=True,
    retain_last=5,
    older_than="2023-10-01 00:00:00"
)
```

## ExpireSnapshotsTask

A specialized wrapper for expiring snapshots.

### Arguments
-   `name` (str): The unique name of the task.
-   `client` (DremioClient): The authenticated Dremio client.
-   `table` (str): The full path to the Iceberg table.
-   `retain_last` (int, default=5): Number of recent snapshots to retain.

### Example
```python
from dremioframe.orchestration import ExpireSnapshotsTask

t_exp = ExpireSnapshotsTask(
    name="expire_sales",
    client=client,
    table="my_catalog.sales",
    retain_last=3
)
```
