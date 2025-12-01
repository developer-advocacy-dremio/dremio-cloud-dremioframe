# DremioFrame (currently in alpha)

DremioFrame is a Python library that provides a dataframe builder interface for interacting with Dremio Cloud & Dremio Software. It allows you to list data, perform CRUD operations, and administer Dremio resources using a familiar API.

## Documentation

### 🚀 Getting Started
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](docs/configuration.md)
- [Connecting to Dremio](docs/connection.md)
- [Tutorial: ETL Pipeline](docs/tutorial_etl.md)
- [Cookbook / Recipes](docs/cookbook.md)
- [Troubleshooting](docs/troubleshooting.md)

### 🛠️ Data Engineering
- [Dataframe Builder API](docs/builder.md)
- [Querying Data](docs/querying.md)
- [Joins & Transformations](docs/joins.md)
- [Aggregation](docs/aggregation.md)
- [Sorting & Filtering](docs/sorting.md)
- [Ingestion API](docs/ingestion.md)
- [Ingestion Patterns](docs/ingestion_patterns.md)
- [Exporting Data](docs/export.md)
- [Working with Files](docs/files.md)
- [Caching](docs/caching.md)
- [Pydantic Integration](docs/pydantic_integration.md)
- [Iceberg Tables](docs/iceberg.md)
- [Iceberg Lakehouse Management](docs/guide_iceberg_management.md)

### 📊 Analysis & Visualization
- [Charting & Plotting](docs/charting.md)
- [Interactive Plotting](docs/plotting.md)
- [Query Profiling](docs/profiling.md)

### 🧠 AI Capabilities
- [Script Generation](docs/ai_generation.md)
- [SQL Generation](docs/ai_sql.md)
- [API Call Generation](docs/ai_api.md)

### 📐 Data Modeling
- [Medallion Architecture](docs/modeling/medallion.md)
- [Dimensional Modeling](docs/modeling/dimensional.md)
- [Slowly Changing Dimensions](docs/modeling/scd.md)
- [Semantic Views](docs/modeling/views.md)
- [Documenting Datasets](docs/modeling/documentation.md)

### ⚙️ Orchestration
- [Overview](docs/orchestration.md)
- [Tasks & Sensors](docs/orchestration_tasks.md)
- [Extensions](docs/orchestration_extensions.md)
- [Scheduling](docs/orchestration_scheduling.md)
- [Dremio Jobs](docs/orchestration_dremio_jobs.md)
- [Iceberg Tasks](docs/orchestration_iceberg.md)
- [Reflection Tasks](docs/orchestration_reflections.md)
- [Data Quality Task](docs/orchestration_dq_task.md)
- [Distributed Execution](docs/orchestration_distributed.md)
- [Deployment](docs/orchestration_deployment.md)
- [CLI & UI](docs/orchestration_cli.md)
- [Web UI](docs/orchestration_ui.md)
- [Backends](docs/orchestration_backend.md)
- [Best Practices](docs/orchestration_best_practices.md)

### ✅ Data Quality
- [DQ Framework](docs/data_quality.md)

### 🔧 Administration & Governance
- [Administration](docs/admin.md)
- [Catalog Management](docs/catalog.md)
- [Reflections Management](docs/guide_reflections.md)
- [User Defined Functions (UDFs)](docs/udf.md)
- [Security Best Practices](docs/security.md)
- [Governance: Masking & Row Access](docs/governance/masking_and_row_access.md)
- [Governance: Tags](docs/governance/tags.md)
- [Governance: Lineage](docs/governance/lineage.md)
- [Governance: Privileges](docs/governance/privileges.md)

### 📚 Reference
- [Function Reference](docs/function_reference.md)
- [SQL Functions Guide](docs/functions.md)
- [CLI Reference](docs/cli.md)
- [API Reference](docs/reference/client.md)
- [Async Client](docs/async_client.md)
- [Advanced Usage](docs/advanced.md)
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
