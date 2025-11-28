# Iceberg Features

DremioFrame provides first-class support for Apache Iceberg features like Time Travel and Table Maintenance.

## Time Travel

Query a table as it existed at a specific point in time.

```python
# Query by Snapshot ID
df.at_snapshot("123456789").show()

# Query by Timestamp
df.at_timestamp("2023-01-01 12:00:00").show()
```

## Table Maintenance

Automate table optimization and cleanup.

### Optimize

Compacts small files to improve query performance.

```python
# Rewrite data files
client.table("my_table").optimize()

# Only optimize if input files are small
client.table("my_table").optimize(min_input_files=5)
```

### Vacuum

Removes old snapshots and data files to free up space.

```python
# Expire snapshots older than default retention
client.table("my_table").vacuum()

# Retain only the last 10 snapshots
client.table("my_table").vacuum(retain_last=10)
```
