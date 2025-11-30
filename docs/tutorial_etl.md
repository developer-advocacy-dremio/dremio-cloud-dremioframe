# Tutorial: Building a Production ETL Pipeline

This tutorial guides you through building a complete ETL pipeline using DremioFrame. You will ingest data, transform it, validate it, and schedule the job.

## Prerequisites
- Dremio Cloud or Software instance.
- Python 3.8+.
- `dremioframe` installed.

## Step 1: Connect to Dremio

Create a script `etl_pipeline.py`.

```python
import os
from dremioframe.client import DremioClient
from dremioframe.orchestration import Pipeline, DremioQueryTask
from dremioframe.orchestration.tasks.dq_task import DataQualityTask
from dremioframe.orchestration.scheduling import schedule_pipeline

# Initialize Client
client = DremioClient(
    pat=os.environ.get("DREMIO_PAT"),
    project_id=os.environ.get("DREMIO_PROJECT_ID")
)
```

## Step 2: Define the Pipeline

```python
pipeline = Pipeline("daily_sales_etl")
```

## Step 3: Ingest Data (Extract & Load)

Assume we are ingesting from an external source into a staging table.

```python
ingest_task = DremioQueryTask(
    name="ingest_sales",
    client=client,
    sql="""
    CREATE TABLE IF NOT EXISTS "Target.staging.sales" AS
    SELECT * FROM "Source.external.sales_data"
    """
)
pipeline.add_task(ingest_task)
```

## Step 4: Transform Data

Clean and aggregate the data.

```python
transform_task = DremioQueryTask(
    name="transform_sales",
    client=client,
    sql="""
    CREATE OR REPLACE TABLE "Target.mart.daily_sales" AS
    SELECT 
        date_trunc('day', sale_date) as sale_day,
        region,
        SUM(amount) as total_revenue
    FROM "Target.staging.sales"
    GROUP BY 1, 2
    """
)
pipeline.add_task(transform_task)

# Set dependency
transform_task.set_upstream(ingest_task)
```

## Alternative: Using DataFrame API (DremioBuilderTask)

Instead of raw SQL, you can use the Pythonic DataFrame API.

```python
from dremioframe.orchestration import DremioBuilderTask

# Define transformation using Builder
builder = client.table("Target.staging.sales") \
    .group_by("sale_date", "region") \
    .agg(total_revenue="SUM(amount)")

# Create task to merge results into mart
transform_task = DremioBuilderTask(
    name="transform_sales_builder",
    builder=builder,
    command="merge",
    target="Target.mart.daily_sales",
    options={
        "on": ["sale_date", "region"],
        "matched_update": {"total_revenue": "source.total_revenue"},
        "not_matched_insert": {
            "sale_date": "source.sale_date",
            "region": "source.region",
            "total_revenue": "source.total_revenue"
        }
    }
)
pipeline.add_task(transform_task)
```

## Step 5: Validate Data (Quality Check)

Ensure the transformed data is valid. Create a test file `tests/dq/sales_checks.yaml`:

```yaml
- name: check_revenue_positive
  table: "Target.mart.daily_sales"
  type: custom_sql
  condition: "total_revenue >= 0"
  error_msg: "Revenue cannot be negative"

- name: check_regions_exist
  table: "Target.mart.daily_sales"
  type: not_null
  column: region
```

Add the DQ task to the pipeline:

```python
dq_task = DataQualityTask(
    name="validate_sales",
    client=client,
    directory="tests/dq"
)
pipeline.add_task(dq_task)

dq_task.set_upstream(transform_task)
```

## Step 6: Run or Schedule

To run immediately:
```python
if __name__ == "__main__":
    pipeline.run()
```

To schedule daily at 2 AM:
```python
if __name__ == "__main__":
    schedule_pipeline(pipeline, "cron", hour=2, minute=0)
```

## Conclusion
You have built a robust pipeline that ingests, transforms, and validates data, with automatic failure handling and scheduling!
