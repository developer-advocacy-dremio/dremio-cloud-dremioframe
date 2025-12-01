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

You can implement your own backend (e.g., Postgres, Redis, S3) by extending `BaseBackend`.

#### The `PipelineRun` Object
Your backend will need to store and retrieve `PipelineRun` objects.
```python
@dataclass
class PipelineRun:
    pipeline_name: str
    run_id: str
    start_time: float
    status: str          # "RUNNING", "SUCCESS", "FAILED"
    end_time: float      # Optional
    tasks: Dict[str, str] # Map of task_name -> status
```

#### Required Methods

You must implement the following 4 methods:

1.  **`save_run(self, run: PipelineRun)`**:
    *   Called when a pipeline starts and finishes.
    *   Should upsert the run record in your storage.

2.  **`get_run(self, run_id: str) -> Optional[PipelineRun]`**:
    *   Called to retrieve a specific run.
    *   Return `None` if not found.

3.  **`update_task_status(self, run_id: str, task_name: str, status: str)`**:
    *   Called every time a task changes state (RUNNING, SUCCESS, FAILED, SKIPPED).
    *   Must be efficient and thread-safe if possible.

4.  **`list_runs(self, pipeline_name: str = None, limit: int = 10) -> List[PipelineRun]`**:
    *   Called by the UI to show history.
    *   Should return the most recent runs, optionally filtered by pipeline name.

#### Example Implementation Skeleton

```python
from dremioframe.orchestration.backend import BaseBackend, PipelineRun
from typing import List, Optional

class MyRedisBackend(BaseBackend):
    def __init__(self, redis_client):
        self.redis = redis_client

    def save_run(self, run: PipelineRun):
        # Serialize run to JSON and save to Redis key `run:{run.run_id}`
        pass
        
    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        # Get JSON from Redis and deserialize to PipelineRun
        pass
        
    def update_task_status(self, run_id: str, task_name: str, status: str):
        # Update the specific field in the stored JSON or Hash
        pass

    def list_runs(self, pipeline_name: str = None, limit: int = 10) -> List[PipelineRun]:
        # Scan keys or use a sorted set for time-based retrieval
        pass
```

## Postgres Backend

The `PostgresBackend` stores pipeline state in a PostgreSQL database.

### Requirements
```bash
pip install "dremioframe[postgres]"
```

### Usage
```python
from dremioframe.orchestration.backend import PostgresBackend

# Uses DREMIOFRAME_PG_DSN env var if dsn not provided
backend = PostgresBackend(dsn="postgresql://user:password@localhost:5432/mydb")
pipeline = Pipeline("my_pipeline", backend=backend)
```

## MySQL Backend

The `MySQLBackend` stores pipeline state in a MySQL database.

### Requirements
```bash
pip install "dremioframe[mysql]"
```

### Usage
```python
from dremioframe.orchestration.backend import MySQLBackend

# Uses DREMIOFRAME_MYSQL_* env vars if config not provided
backend = MySQLBackend(config={
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "database": "mydb"
})
pipeline = Pipeline("my_pipeline", backend=backend)
```
