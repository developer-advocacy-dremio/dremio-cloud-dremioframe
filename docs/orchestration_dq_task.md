# Data Quality Task

The `DataQualityTask` integrates the Data Quality Framework into your orchestration pipelines. It allows you to run a suite of DQ checks as a step in your DAG. If any check fails, the task fails, halting the pipeline (unless handled).

## Usage

```python
from dremioframe.orchestration import Pipeline
from dremioframe.orchestration.tasks.dq_task import DataQualityTask
from dremioframe.client import DremioClient

client = DremioClient()
pipeline = Pipeline("dq_pipeline")

# Run checks from a directory
dq_task = DataQualityTask(
    name="run_sales_checks",
    client=client,
    directory="tests/dq"
)

pipeline.add_task(dq_task)
pipeline.run()
```

## Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `name` | `str` | Name of the task. |
| `client` | `DremioClient` | Authenticated Dremio client. |
| `directory` | `str` | Path to a directory containing YAML test files. |
| `tests` | `list` | List of test dictionaries (alternative to directory). |

## Behavior

- **Success**: If all checks pass, the task completes successfully.
- **Failure**: If any check fails, the task raises a `RuntimeError`, marking the task as failed.
