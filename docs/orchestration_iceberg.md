# Iceberg Maintenance Tasks

`dremioframe` simplifies Iceberg table maintenance with pre-built tasks.

## OptimizeTask

Runs `OPTIMIZE TABLE` to compact small files.

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

```python
from dremioframe.orchestration import VacuumTask

t_vac = VacuumTask(
    name="vacuum_sales",
    client=client,
    table="my_catalog.sales",
    expire_snapshots=True,
    retain_last=5
)
```

## ExpireSnapshotsTask

A specialized wrapper for expiring snapshots.

```python
from dremioframe.orchestration import ExpireSnapshotsTask

t_exp = ExpireSnapshotsTask(
    name="expire_sales",
    client=client,
    table="my_catalog.sales",
    retain_last=3
)
```
