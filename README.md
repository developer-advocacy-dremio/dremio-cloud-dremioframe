# DremioFrame (currently in alpha)

DremioFrame is a Python library that provides a dataframe builder interface for interacting with Dremio Cloud & Dremio Software. It allows you to list data, perform CRUD operations, and administer Dremio resources using a familiar API.

## Documentation

- [Architecture](architecture.md)

- [Connection Guide](docs/connection.md)
- [Administration](docs/admin.md)
- [Catalog & Admin](docs/catalog.md)
- [Dataframe Builder](docs/builder.md)
- [Aggregation](docs/aggregation.md)
- [Sorting & Distinct](docs/sorting.md)
- [Joins](docs/joins.md)
- [Iceberg Features](docs/iceberg.md)
- [Advanced Features](docs/advanced.md)
- [Charting](docs/charting.md)
- [Data Export](docs/export.md)
- [API Ingestion](docs/ingestion.md)
- [Ingestion Patterns](docs/ingestion_patterns.md)
- [Working with Files](docs/files.md)
- [SQL Functions](docs/functions.md)
    - [Aggregate](docs/functions/aggregate.md)
    - [Math](docs/functions/math.md)
    - [String](docs/functions/string.md)
    - [Date](docs/functions/date.md)
    - [Window](docs/functions/window.md)
    - [Conditional](docs/functions/conditional.md)
    - [AI](docs/functions/ai.md)
    - [Complex Types](docs/functions/complex.md)
- [Local Caching](docs/caching.md)
- [Interactive Plotting](docs/plotting.md)
- [Raw SQL Querying](docs/querying.md)
- [UDF Manager](docs/udf.md)
- [CLI Tool](docs/cli.md)
- [Async Client](docs/async_client.md)

## Installation

```bash
pip install dremioframe
```

## Quick Start

### Dremio Cloud

```python
from dremioframe.client import DremioClient

# Assumes DREMIO_PAT and DREMIO_PROJECT_ID are set in env
client = DremioClient()

# Query a table
df = client.table("Samples.samples.dremio.com.zips.json").select("city", "state").limit(5).collect()
print(df)
```

### Dremio Software

```python
client = DremioClient(
    hostname="localhost",
    port=32010,
    username="admin",
    password="password123",
    tls=False
)
```

## Features

```python
from dremioframe.client import DremioClient

client = DremioClient(pat="YOUR_PAT", project_id="YOUR_PROJECT_ID")

# List catalog
print(client.catalog.list_catalog())

# Query data
df = client.table("Samples.samples.dremio.com.zips.json").select("city", "state").filter("state = 'MA'").collect()
print(df)

# Calculated Columns
df.mutate(total_pop="pop * 2").show()

# Aggregation
df.group_by("state").agg(avg_pop="AVG(pop)").show()

# Joins
df.join("other_table", on="left_tbl.id = right_tbl.id").show()

# Iceberg Time Travel
df.at_snapshot("123456789").show()



# API Ingestion
client.ingest_api(
    url="https://api.example.com/users",
    table_name="users",
    mode="merge",
    pk="id"
)

# Charting
df.chart(kind="bar", x="category", y="sales", save_to="sales.png")

# Export
df.to_csv("data.csv")
df.to_parquet("data.parquet")

# Insert Data (Batched)
import pandas as pd
data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
client.table("my_table").insert("my_table", data=data, batch_size=1000)

# SQL Functions
from dremioframe import F

client.table("sales") \
    .select(
        F.col("dept"),
        F.sum("amount").alias("total_sales"),
        F.rank().over(F.Window.order_by("amount")).alias("rank")
    ) \
    .show()

# Merge (Upsert)
client.table("target").merge(
    target_table="target",
    on="id",
    matched_update={"name": "source.name"},
    not_matched_insert={"id": "source.id", "val": "source.val"},
    data=data
)

# Data Quality
df.quality.expect_not_null("city")
df.quality.expect_row_count("pop > 1000000", 5, "ge") # Expect at least 5 cities with pop > 1M

# Query Explanation
print(df.explain())

# Reflection Management
client.admin.create_reflection(dataset_id="...", name="my_ref", type="RAW", display_fields=["col1"])

# Async Client
# async with AsyncDremioClient(pat="...") as client: ...

# CLI
# dremio-cli query "SELECT 1"

# Local Caching
# client.table("source").cache("my_cache", ttl_seconds=300).sql("SELECT * FROM my_cache").show()

# Interactive Plotting
# df.chart(kind="scatter", backend="plotly").show()

# UDF Manager
# client.udf.create("add_one", {"x": "INT"}, "INT", "x + 1")

# Raw SQL
# df = client.query("SELECT * FROM my_table")
```
