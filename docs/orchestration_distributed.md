# Distributed Execution

DremioFrame Orchestration supports distributed task execution using **Celery**. This allows you to scale your pipelines across multiple worker nodes.

## Executors

The `Pipeline` class now accepts an `executor` argument.

### LocalExecutor (Default)

Executes tasks locally using a thread pool.

```python
from dremioframe.orchestration import Pipeline
from dremioframe.orchestration.executors import LocalExecutor

# Default behavior (uses LocalExecutor with 1 worker)
pipeline = Pipeline("my_pipeline")

# Explicitly configure LocalExecutor
executor = LocalExecutor(backend=backend, max_workers=4)
pipeline = Pipeline("my_pipeline", executor=executor)
```

### CeleryExecutor

Executes tasks on a Celery cluster. This requires a message broker (like Redis or RabbitMQ).

#### Requirements
```bash
pip install "dremioframe[celery]"
```

#### Configuration

1.  **Start a Redis Server** (or other broker).
2.  **Start a Celery Worker**:
    You need a worker process that can import `dremioframe` and your task code.
    
    Create a `worker.py`:
    ```python
    from celery import Celery
    
    # Configure the app to match the executor's settings
    app = Celery("dremioframe_orchestration", broker="redis://localhost:6379/0")
    app.conf.update(
        result_backend="redis://localhost:6379/0",
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        imports=["dremioframe.orchestration.executors"] # Important!
    )
    ```
    
    Run the worker:
    ```bash
    celery -A worker worker --loglevel=info
    ```

3.  **Configure the Pipeline**:
    ```python
    from dremioframe.orchestration import Pipeline
    from dremioframe.orchestration.executors import CeleryExecutor
    
    executor = CeleryExecutor(backend=backend, broker_url="redis://localhost:6379/0")
    pipeline = Pipeline("my_pipeline", executor=executor)
    
    pipeline.run()
    ```

## Task Serialization

The `CeleryExecutor` uses `pickle` to serialize your task objects and their actions. 
**Important**: Ensure your task actions are top-level functions or importable callables. Lambdas and nested functions may fail to pickle.
