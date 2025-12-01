# DremioFrame (currently in alpha)

DremioFrame is a Python library that provides a dataframe builder interface for interacting with Dremio Cloud & Dremio Software. It allows you to list data, perform CRUD operations, and administer Dremio resources using a familiar API.

## Documentation

### 🚀 Getting Started
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](docs/configuration.md)
- [Tutorial: ETL Pipeline](docs/tutorial_etl.md)
- [Cookbook / Recipes](docs/cookbook.md)
- [Data Modeling](docs/modeling/medallion.md)
    - [Medallion Architecture](docs/modeling/medallion.md)
    - [Dimensional Modeling](docs/modeling/dimensional.md)
    - [Slowly Changing Dimensions](docs/modeling/scd.md)
    - [Semantic Views](docs/modeling/views.md)
    - [Documenting Datasets](docs/modeling/documentation.md)
- [Security Best Practices](docs/security.md)

### 🛠️ Data Engineering
- [Dataframe Builder API](docs/builder.md)
- [Iceberg Lakehouse Management](docs/guide_iceberg_management.md)
- [SCD2 Helper Guide](docs/scd2_guide.md)
- [Pydantic Integration](docs/pydantic_integration.md)
- [Ingestion Patterns](docs/ingestion_patterns.md)
- [SQL Functions](docs/functions.md)

### ⚙️ Orchestration
- [Overview](docs/orchestration.md)
- [Tasks & Sensors](docs/orchestration_extensions.md)
- [Deployment](docs/orchestration_deployment.md)
- [Distributed Execution](docs/orchestration_distributed.md)
- [CLI & UI](docs/orchestration_cli.md)

### ✅ Data Quality
- [DQ Framework](docs/data_quality.md)
- [DQ Task](docs/orchestration_dq_task.md)

### 🔧 Administration
- [Reflections Management](docs/guide_reflections.md)
- [Catalog Management](docs/catalog.md)
- [Governance (Masking & Row Access)](docs/governance/masking_and_row_access.md)
- [Troubleshooting](docs/troubleshooting.md)

### 📚 Reference
- [API Reference](docs/reference/client.md)
- [Architecture](architecture.md)
- [Testing Guide](docs/testing.md)
- [Contributing](CONTRIBUTING.md)

## Installation

```bash
pip install dremioframe
```

To install with optional dependencies (e.g., for static image export):
```bash
pip install "dremioframe[image_export]"
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

# Source Management
# client.admin.create_source_s3("my_datalake", "bucket")

# Query Profiling
# client.admin.get_job_profile("job_123").visualize().show()

# Iceberg Client
# client.iceberg.list_tables("my_namespace")

# Orchestration CLI
# dremio-cli pipeline list
# dremio-cli pipeline ui --port 8080

# Data Quality Framework
# dremio-cli dq run tests/dq
```
