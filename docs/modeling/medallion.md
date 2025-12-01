# Medallion Architecture

The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally improving the quality and structure of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold).

## Bronze Layer (Raw)

The **Bronze** layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc.

### Characteristics
- **Raw Data**: Data is ingested in its native format.
- **Append-Only**: New data is appended; history is preserved.
- **No Validation**: Minimal to no data validation is performed.

### Example: Ingesting Raw Logs

```python
from dremioframe.client import DremioClient
client = DremioClient()

# Ingest raw JSON logs from an S3 source into the Bronze layer
# We use CTAS to create a table from the raw files
client.sql("""
    CREATE TABLE "bronze"."app_logs"
    AS SELECT 
        *,
        CURRENT_TIMESTAMP as _ingestion_time
    FROM "s3_source"."bucket"."raw_logs"
""")
```

## Silver Layer (Cleaned & Conformed)

In the **Silver** layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleaned ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions.

### Characteristics
- **Cleaned**: Nulls handled, types cast, formatting standardized.
- **Deduplicated**: Duplicate records are removed.
- **Enriched**: Data may be joined with reference data.

### Example: Cleaning and Deduplicating

```python
from dremioframe import F

# Read from Bronze
df_bronze = client.table("bronze.app_logs")

# Transformation Logic
df_silver = df_bronze \
    .filter("user_id IS NOT NULL") \
    .select(
        "user_id",
        F.to_timestamp("event_time").alias("event_time"),
        F.lower("event_type").alias("event_type"),
        "metadata" # Keeping JSON struct
    ) \
    .drop_duplicates(["user_id", "event_time"])

# Materialize to Silver
df_silver.create("silver.app_events")
```

## Gold Layer (Curated)

Data in the **Gold** layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins.

### Characteristics
- **Aggregated**: Business-level aggregates (e.g., Daily Active Users).
- **Dimensional**: Star schemas (Fact and Dimension tables).
- **Business Logic**: Complex business rules applied.

### Example: Daily Active Users (DAU)

```python
# Read from Silver
df_silver = client.table("silver.app_events")

# Calculate DAU
df_gold = df_silver \
    .group_by(F.to_date("event_time").alias("date")) \
    .agg(
        dau=F.count("user_id"),
        events_count=F.count("*")
    )

# Materialize to Gold
df_gold.create("gold.daily_active_users")
```

## Folder Structure

A common pattern in Dremio is to use Spaces or Folders to represent these layers:

- **Space: `Bronze`** (or `Raw`)
- **Space: `Silver`** (or `Staging`)
- **Space: `Gold`** (or `Curated`)

You can manage these spaces programmatically:

```python
# Create spaces if they don't exist
for layer in ["Bronze", "Silver", "Gold"]:
    try:
        client.catalog.create_space(layer)
    except:
        pass # Already exists
```
