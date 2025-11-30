# Dremio Job Integration

`dremioframe` provides specialized tasks for interacting with Dremio Jobs.

## DremioQueryTask

The `DremioQueryTask` submits a SQL query to Dremio, waits for its completion, and supports cancellation.

### Features
-   **Job Tracking**: Tracks the Dremio Job ID.
-   **Cancellation**: If the pipeline is killed or the task is cancelled, it attempts to cancel the running Dremio Job.
-   **Polling**: Efficiently polls for job status.

### Usage

```python
from dremioframe.orchestration import DremioQueryTask, Pipeline
from dremioframe.client import DremioClient

client = DremioClient(...)

# Create a task
t1 = DremioQueryTask(
    name="run_heavy_query",
    client=client,
    sql="SELECT * FROM my_heavy_table"
)

pipeline = Pipeline("dremio_pipeline")
pipeline.add_task(t1)
pipeline.run()
```
