# Guide: Reflections Management

Reflections are Dremio's query acceleration technology. DremioFrame allows you to manage them programmatically.

## Listing Reflections

View all reflections in your Dremio environment.

```python
from dremioframe.client import DremioClient

client = DremioClient(...)

# List all reflections
reflections = client.admin.list_reflections()
for r in reflections['data']:
    print(f"Name: {r['name']}, Status: {r['status']['availability']}")
```

## Creating Reflections

You can create Raw or Aggregation reflections on a dataset.

```python
# Create a Raw Reflection
client.admin.create_reflection(
    dataset_id="<dataset-uuid>",
    name="raw_sales_reflection",
    type="RAW",
    display_fields=["sale_date", "amount", "region"]
)

# Create an Aggregation Reflection
client.admin.create_reflection(
    dataset_id="<dataset-uuid>",
    name="agg_sales_by_region",
    type="AGGREGATION",
    dimension_fields=["region", "sale_date"],
    measure_fields=["amount"]
)
```

## Refreshing Reflections

Trigger an immediate refresh of a reflection.

```python
client.admin.refresh_reflection("<reflection-uuid>")
```

## Automating Refreshes

While Dremio has internal scheduling, you might want to trigger refreshes as part of an external pipeline (e.g., immediately after data ingestion).

```python
from dremioframe.orchestration import Pipeline, RefreshReflectionTask

pipeline = Pipeline("ingest_and_refresh")

# ... Ingestion tasks ...

refresh = RefreshReflectionTask(
    name="refresh_sales_agg",
    client=client,
    reflection_id="<reflection-uuid>"
)

pipeline.add_task(refresh)
# refresh.set_upstream(ingest_task)

pipeline.run()
```

## Checking Status

You can poll for reflection status to ensure it's available.

```python
status = client.admin.get_reflection("<reflection-uuid>")
print(status['status']['availability']) # e.g., 'AVAILABLE', 'EXPIRING', 'FAILED'
```
