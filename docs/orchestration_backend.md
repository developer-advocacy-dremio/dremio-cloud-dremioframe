# Orchestration Backend

By default, `dremioframe` pipelines store their state in memory. This means if the process exits, the history of pipeline runs is lost.
To persist pipeline history and enable features like the Web UI, you can use a persistent backend.

## SQLite Backend

The `SQLiteBackend` stores pipeline runs and task statuses in a local SQLite database file.

### Usage

```python
from dremioframe.orchestration import Pipeline
from dremioframe.orchestration.backend import SQLiteBackend

# Initialize backend
backend = SQLiteBackend(db_path="pipeline_history.db")

# Pass backend to Pipeline
pipeline = Pipeline("my_pipeline", backend=backend)

pipeline.run()
```

### Custom Backends

You can implement your own backend (e.g., Postgres, Redis) by extending `BaseBackend`.

```python
from dremioframe.orchestration.backend import BaseBackend, PipelineRun

class MyCustomBackend(BaseBackend):
    def save_run(self, run: PipelineRun):
        # Save to your DB
        pass
        
    def get_run(self, run_id: str):
        # Retrieve from your DB
        pass
        
    def update_task_status(self, run_id: str, task_name: str, status: str):
        # Update status
        pass

    def list_runs(self, pipeline_name: str = None, limit: int = 10):
        # List runs
        pass
```
