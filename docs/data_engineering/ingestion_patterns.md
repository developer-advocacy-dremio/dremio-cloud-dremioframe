# Ingestion Patterns & Best Practices

This guide outlines common patterns for moving data into Iceberg tables using DremioFrame, covering both Dremio-connected sources and external data.

## Why Move Data to Iceberg?

While Dremio can query data directly from sources like Postgres, SQL Server, or S3, moving data into **Apache Iceberg** tables (in Dremio's Arctic or S3/Data Lake sources) offers significant benefits:
- **Performance**: Iceberg tables are optimized for analytics (columnar, partitioned).
- **Features**: Enables Time Travel, Rollback, and DML operations (Update/Delete/Merge).
- **Isolation**: Decouples analytical workloads from operational databases.

---

## Pattern 1: Source to Iceberg (ELT)

If your data is already in a source connected to Dremio (e.g., a Postgres database or a raw S3 folder), you can use Dremio to move it into an Iceberg table.

### Initial Load (CTAS)

Use the `create` method to perform a `CREATE TABLE AS SELECT` (CTAS) operation. This pushes the work to the Dremio engine.

```python
# Create an Iceberg table 'marketing.users' from a Postgres source table
client.table("postgres.public.users") \
    .filter("active = true") \
    .create("marketing.users")
```

### Incremental Append

Use `insert` to append new rows from a source to an existing Iceberg table.

```python
# Append new logs from S3 to Iceberg
client.table("s3.raw_logs") \
    .filter("event_date = CURRENT_DATE") \
    .insert("marketing.logs")
```

### Upsert (Merge)

Use `merge` to update existing records and insert new ones.

```python
# Upsert users from Postgres to Iceberg
client.table("postgres.public.users").merge(
    target_table="marketing.users",
    on="id",
    matched_update={"email": "source.email", "status": "source.status"},
    not_matched_insert={"id": "source.id", "email": "source.email", "status": "source.status"}
)
```

---

## Pattern 2: External Data to Iceberg (ETL)

If your data originates outside Dremio (e.g., REST APIs, local files, Python scripts), you can ingest it using DremioFrame.

### API Ingestion

Use the `ingest_api` utility for REST APIs.

```python
# Fetch users from an API and merge them into an Iceberg table
client.ingest_api(
    url="https://api.example.com/users",
    table_name="marketing.users",
    mode="merge",
    pk="id"
)
```

### Local Dataframes (Pandas/Arrow)

If you have data in a Pandas DataFrame or PyArrow Table, the recommended approach for creating new tables is `client.create_table`.

```python
import pandas as pd

# Load local CSV
df = pd.read_csv("local_data.csv")

# Option 1: Using create_table (Recommended for new tables)
# This is the cleanest API for creating tables from local data
client.create_table("marketing.local_data", schema=df, insert_data=True)

# Option 2: Using builder.create (CTAS approach)
# Note: The source table in client.table() is ignored when 'data' is provided.
# This pattern is useful if you are already working with a builder object.
client.table("marketing.local_data").create("marketing.local_data", data=df)

# Option 3: Appending to existing table
# Use this to add data to an existing table
client.table("marketing.local_data").insert("marketing.local_data", data=df)
```

**Note**: For large local datasets, use the `batch_size` parameter to avoid memory issues and timeouts.

```python
client.table("target").insert("target", data=large_df, batch_size=5000)
```

See [Creating Tables](creating_tables.md) for more details on table creation methods.

---

## Best Practices

### 1. Optimize Your Tables
After significant data ingestion (especially many small inserts), run `optimize()` to compact small files.

```python
client.table("marketing.users").optimize()
```

### 2. Manage Snapshots
Iceberg keeps history for Time Travel. To save storage, periodically expire old snapshots using `vacuum()`.

```python
# Retain only the last 5 snapshots
client.table("marketing.users").vacuum(retain_last=5)
```

### 3. Use Staging Tables for Complex Merges
If you need to perform complex transformations before merging, load data into a temporary staging table first.

```python
# 1. Load raw data to staging
client.ingest_api(..., table_name="staging_users", mode="replace")

# 2. Transform and Merge from staging to target
client.table("staging_users") \
    .mutate(full_name="concat(first, ' ', last)") \
    .merge(target_table="marketing.users", on="id", ...)

# 3. Drop staging
client.table("staging_users").delete() # Or drop via SQL
```

### 4. Batching
When inserting data from Python (Pandas/Arrow), always use `batch_size` for datasets larger than a few thousand rows.

### 5. Type Consistency
Ensure your local DataFrame types match Dremio's expected types. DremioFrame handles basic conversion, but explicit casting in Pandas (e.g., `pd.to_datetime`) is recommended before ingestion.
