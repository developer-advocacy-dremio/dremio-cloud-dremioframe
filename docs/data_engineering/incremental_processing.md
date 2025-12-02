# Incremental Processing

DremioFrame simplifies incremental data loading patterns, allowing you to efficiently process only new or changed data.

## IncrementalLoader

The `IncrementalLoader` class provides helper methods for watermark-based loading and MERGE (upsert) operations.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.incremental import IncrementalLoader

client = DremioClient()
loader = IncrementalLoader(client)
```

### Watermark-based Loading

This pattern loads data from a source table to a target table where a specific column (e.g., timestamp or ID) is greater than the maximum value in the target table.

```python
# Load new data from 'staging.events' to 'analytics.events'
# based on the 'event_time' column.
rows_inserted = loader.load_incremental(
    source_table="staging.events",
    target_table="analytics.events",
    watermark_col="event_time"
)

print(f"Loaded {rows_inserted} new rows.")
```

**How it works:**
1. Queries `MAX(event_time)` from `analytics.events`.
2. Executes `INSERT INTO analytics.events SELECT * FROM staging.events WHERE event_time > 'MAX_VALUE'`.
3. If the target table is empty, it performs a full load.

### Merge (Upsert)

The `merge` method performs a standard SQL MERGE operation to update existing records and insert new ones.

```python
# Upsert users from staging to production
loader.merge(
    source_table="staging.users",
    target_table="production.users",
    on=["user_id"],                 # Join condition
    update_cols=["email", "status"], # Columns to update when matched
    insert_cols=["user_id", "email", "status", "created_at"] # Columns to insert when not matched
)
```

**Generated SQL:**
```sql
MERGE INTO production.users AS target 
USING staging.users AS source 
ON (target.user_id = source.user_id)
WHEN MATCHED THEN 
    UPDATE SET email = source.email, status = source.status
WHEN NOT MATCHED THEN 
    INSERT (user_id, email, status, created_at) 
    VALUES (source.user_id, source.email, source.status, source.created_at)
```

## Best Practices

- **Indexing**: Ensure your watermark columns and join keys are optimized (e.g., sorted or partitioned) in Dremio for performance.
- **Reflections**: Use Dremio Reflections to accelerate the `MAX(watermark)` query on large target tables.
- **Data Types**: Ensure data types match between source and target to avoid casting issues during INSERT/MERGE.
