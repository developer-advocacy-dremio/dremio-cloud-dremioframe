# DremioFrame (currently in alpha)

DremioFrame is a Python library that provides a dataframe builder interface for interacting with Dremio Cloud & Dremio Software. It allows you to list data, perform CRUD operations, and administer Dremio resources using a familiar API.

## Documentation

### 🚀 Getting Started
- [Installation](#installation)
- [Optional Dependencies](docs/getting_started/dependencies.md)
- [S3 Integration](docs/getting_started/s3_integration.md)
- [Quick Start](#quick-start)
- [Configuration](docs/getting_started/configuration.md)
- [Connecting to Dremio](docs/getting_started/connection.md)
- [Tutorial: ETL Pipeline](docs/getting_started/tutorial_etl.md)
- [Cookbook / Recipes](docs/getting_started/cookbook.md)
- [Troubleshooting](docs/getting_started/troubleshooting.md)

### 🛠️ Data Engineering
- [Dataframe Builder API](docs/data_engineering/builder.md)
- [Querying Data](docs/data_engineering/querying.md)
- [Joins & Transformations](docs/data_engineering/joins.md)
- [Aggregation](docs/data_engineering/aggregation.md)
- [Sorting & Filtering](docs/data_engineering/sorting.md)
- [Ingestion API](docs/data_engineering/ingestion.md)
- [Ingestion Patterns](docs/data_engineering/ingestion_patterns.md)
- [dlt Integration](docs/data_engineering/dlt_integration.md)
- [Database Ingestion](docs/data_engineering/database_ingestion.md)
- [File System Ingestion](docs/data_engineering/file_system_ingestion.md)
- [File Upload](docs/data_engineering/file_upload.md)
- [Exporting Data](docs/data_engineering/export.md)
- [Working with Files](docs/data_engineering/files.md)
- [Caching](docs/data_engineering/caching.md)
- [Pydantic Integration](docs/data_engineering/pydantic_integration.md)
- [Iceberg Tables](docs/data_engineering/iceberg.md)
- [Iceberg Lakehouse Management](docs/data_engineering/guide_iceberg_management.md)
- [Schema Evolution](docs/data_engineering/schema_evolution.md)
- [Incremental Processing](docs/data_engineering/incremental_processing.md)
- [Query Templates](docs/data_engineering/query_templates.md)
- [Export Formats](docs/data_engineering/export_formats.md)
- [SQL Linting](docs/data_engineering/sql_linting.md)

### 📊 Analysis & Visualization
- [Charting & Plotting](docs/analysis/charting.md)
- [Interactive Plotting](docs/analysis/plotting.md)
- [Query Profiling](docs/analysis/profiling.md)

### 🧠 AI Capabilities and AI Agent
**note:** this libraries embdedded agent is primarily meant as a code generation assist tool, not meant as an alternative to the integrated Dremio agent for deeper administration and natural language analytics. Login to your Dremio instance's UI to leverage integrated agent.
- [Overview](docs/ai/overview.md)
- [DremioAgent Class](docs/ai/agent.md)
- [MCP Server Integration](docs/ai/mcp_integration.md)
- [Document Extraction](docs/ai/document_extraction.md)
- [Script Generation](docs/ai/generation.md)
- [SQL Generation](docs/ai/sql.md)
- [API Call Generation](docs/ai/api.md)
- [Observability](docs/ai/observability.md)
- [Reflections](docs/ai/reflections.md)
- [Governance](docs/ai/governance.md)
- [Data Quality](docs/ai/data_quality.md)
- [SQL Optimization](docs/ai/optimization.md)
- [CLI Chat](docs/ai/cli_chat.md)

### 📐 Data Modeling
- [Medallion Architecture](docs/modeling/medallion.md)
- [Dimensional Modeling](docs/modeling/dimensional.md)
- [Slowly Changing Dimensions](docs/modeling/scd.md)
- [Semantic Views](docs/modeling/views.md)
- [Documenting Datasets](docs/modeling/documentation.md)

### ⚙️ Orchestration
- [Overview](docs/orchestration/overview.md)
- [Tasks & Sensors](docs/orchestration/tasks.md)
- [Extensions](docs/orchestration/extensions.md)
- [Scheduling](docs/orchestration/scheduling.md)
- [Dremio Jobs](docs/orchestration/dremio_jobs.md)
- [Iceberg Tasks](docs/orchestration/iceberg.md)
- [Reflection Tasks](docs/orchestration/reflections.md)
- [Data Quality Task](docs/orchestration/dq_task.md)
- [Distributed Execution](docs/orchestration/distributed.md)
- [Deployment](docs/orchestration/deployment.md)
- [CLI & UI](docs/orchestration/cli.md)
- [Web UI](docs/orchestration/ui.md)
- [Backends](docs/orchestration/backend.md)
- [Best Practices](docs/orchestration/best_practices.md)

### ✅ Data Quality
- [DQ Framework](docs/data_quality.md)
- [YAML Syntax](docs/data_quality/yaml_syntax.md)
- [Recipes](docs/data_quality/recipes.md)

### 🔧 Administration & Governance
- [Administration](docs/admin_governance/admin.md)
- [Catalog Management](docs/admin_governance/catalog.md)
- [Reflections Management](docs/admin_governance/reflections.md)
- [User Defined Functions (UDFs)](docs/admin_governance/udf.md)
- [Security Best Practices](docs/admin_governance/security.md)
- [Security Patterns](docs/admin_governance/security_patterns.md)
- [Governance: Masking & Row Access](docs/admin_governance/masking_and_row_access.md)
- [Governance: Tags](docs/admin_governance/tags.md)
- [Governance: Lineage](docs/admin_governance/lineage.md)
- [Governance: Privileges](docs/admin_governance/privileges.md)
- [Space & Folder Management](docs/admin_governance/spaces_folders.md)
- [Batch Operations](docs/admin_governance/batch_operations.md)
- [Lineage Tracking](docs/admin_governance/lineage_tracking.md)

### 🔗 Integrations
- [Airflow Integration](docs/integrations/airflow.md)
- [Notebook Integration](docs/integrations/notebook.md)

### 🚀 Performance & Deployment
- [Performance Tuning](docs/performance/tuning.md)
- [Bulk Loading Optimization](docs/performance/bulk_loading.md)
- [Connection Pooling](docs/performance/connection_pooling.md)
- [Query Cost Estimation](docs/performance/cost_estimation.md)
- [CI/CD & Deployment](docs/deployment/cicd.md)

### 📚 Reference
- [Function Reference](docs/reference/function_reference.md)
- [SQL Functions Guide](docs/reference/functions_guide.md)
- [CLI Reference](docs/reference/cli.md)
- [API Reference](docs/reference/client.md)
- [Async Client](docs/reference/async_client.md)

### 🧪 Testing
- [Mock/Testing Framework](docs/testing/mocking.md)
- [Advanced Usage](docs/reference/advanced.md)
- [Architecture](architecture.md)
- [Testing Guide](docs/reference/testing.md)
- [Contributing](CONTRIBUTING.md)

## Installation
 
> [!NOTE]
> DremioFrame has many optional dependencies for advanced features like AI, Chart Exporting, and Distributed Orchestration. See [Optional Dependencies](docs/getting_started/dependencies.md) for a full list.
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
df = client.table('Samples."samples.dremio.com".zips.json').select("city", "state").limit(5).collect()
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
df = client.table('Samples."samples.dremio.com".zips.json').select("city", "state").filter("state = 'MA'").collect()
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
