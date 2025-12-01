# Guide: Iceberg Lakehouse Management

DremioFrame provides powerful tools to manage your Iceberg tables directly from Python. This guide covers maintenance tasks, snapshot management, and time travel.

## Table Maintenance

Regular maintenance is crucial for Iceberg table performance.

### Optimization (Compaction)

Compacts small files into larger ones to improve read performance.

```python
from dremioframe.client import DremioClient

client = DremioClient(...)

# Optimize a specific table
client.table("warehouse.sales").optimize()

# Optimize with specific file size target (if supported by Dremio version/source)
# client.table("warehouse.sales").optimize(target_size_mb=128)
```

### Vacuum (Expire Snapshots & Remove Orphan Files)

Removes old snapshots and unused data files to reclaim space.

```python
# Expire snapshots older than 7 days
client.table("warehouse.sales").vacuum(retain_days=7)
```

## Snapshot Management

### Viewing Snapshots

Inspect the history of your table.

```python
history = client.table("warehouse.sales").history()
print(history)
# Returns a DataFrame with snapshot_id, committed_at, etc.
```

### Time Travel

Query the table as it existed at a specific point in time.

```python
# Query by Snapshot ID
df_snapshot = client.table("warehouse.sales").at(snapshot_id=123456789).collect()

# Query by Timestamp
df_time = client.table("warehouse.sales").at(timestamp="2023-01-01 12:00:00").collect()
```

### Rollback

Revert the table state to a previous snapshot.

```python
# Rollback to a specific snapshot
client.table("warehouse.sales").rollback(snapshot_id=123456789)
```

## Orchestrating Maintenance

You can automate these tasks using DremioFrame's Orchestration features.

```python
from dremioframe.orchestration import Pipeline, OptimizeTask, VacuumTask

pipeline = Pipeline("weekly_maintenance")

optimize = OptimizeTask(
    name="optimize_sales",
    client=client,
    table="warehouse.sales"
)

vacuum = VacuumTask(
    name="vacuum_sales",
    client=client,
    table="warehouse.sales",
    retain_days=7
)

pipeline.add_task(optimize)
pipeline.add_task(vacuum)

# Ensure vacuum runs after optimize
vacuum.set_upstream(optimize)

pipeline.run()
```
