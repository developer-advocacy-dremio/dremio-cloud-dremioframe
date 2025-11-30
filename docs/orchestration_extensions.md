# Orchestration Extensions

DremioFrame includes advanced tasks for orchestration, including dbt integration and sensors.

## dbt Task

The `DbtTask` allows you to run dbt commands within your pipeline.

```python
from dremioframe.orchestration import Pipeline, DbtTask

pipeline = Pipeline("dbt_pipeline")

dbt_run = DbtTask(
    name="run_models",
    command="run",
    project_dir="/path/to/dbt/project",
    select="my_model+"
)

pipeline.add_task(dbt_run)
pipeline.run()
```

## Sensors

Sensors are tasks that wait for a condition to be met before proceeding.

### SqlSensor

Polls a SQL query until it returns data (or a specific condition).

```python
from dremioframe.orchestration import Pipeline, SqlSensor

pipeline = Pipeline("sensor_pipeline")

# Wait until data arrives in staging table
wait_for_data = SqlSensor(
    name="wait_for_staging",
    client=client,
    sql="SELECT 1 FROM staging_table LIMIT 1",
    poke_interval=60, # Check every 60 seconds
    timeout=3600      # Timeout after 1 hour
)

pipeline.add_task(wait_for_data)
pipeline.run()
```

### FileSensor

Checks for the existence of a file in a Dremio source.

```python
from dremioframe.orchestration import Pipeline, FileSensor

# Wait for file to appear
wait_for_file = FileSensor(
    name="wait_for_file",
    client=client,
    path="s3_source.bucket.folder",
    poke_interval=60
)

pipeline.add_task(wait_for_file)
pipeline.run()
```
