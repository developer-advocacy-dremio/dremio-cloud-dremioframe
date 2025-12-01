# Orchestration

DremioFrame includes a lightweight orchestration engine to define, schedule, and run sequences of tasks (DAGs). This allows you to build reliable data pipelines directly within your Python application.

## Core Concepts

### Task
A unit of work, typically wrapping a Python function. Tasks can have dependencies, retries, and can pass data to downstream tasks.

### Pipeline
A collection of tasks with defined dependencies. The pipeline manages execution, ensuring tasks run in the correct order (topological sort) and handling parallel execution.

## Usage

### Basic Example

```python
from dremioframe.orchestration import Task, Pipeline

def step_1():
    print("Step 1")

def step_2():
    print("Step 2")

t1 = Task("step_1", step_1)
t2 = Task("step_2", step_2)
t1.set_downstream(t2)

pipeline = Pipeline("my_pipeline")
pipeline.add_task(t1).add_task(t2)
pipeline.run()
```

### Decorator API

You can use the `@task` decorator for cleaner syntax:

```python
from dremioframe.orchestration import task, Pipeline

@task(name="extract", retries=3)
def extract():
    return [1, 2, 3]

@task(name="transform")
def transform(context=None):
    data = context.get("extract")
    return [x * 2 for x in data]

t_extract = extract()
t_transform = transform()
t_extract.set_downstream(t_transform)

pipeline = Pipeline("etl")
pipeline.add_task(t_extract).add_task(t_transform)
pipeline.run()
```

### Parallel Execution

Specify `max_workers` in the `Pipeline` constructor to run independent tasks in parallel:

```python
pipeline = Pipeline("parallel_etl", max_workers=4)
```

### Visualization

You can generate a Mermaid graph of your pipeline:

```python
print(pipeline.visualize())
# or save to file
pipeline.visualize("pipeline.mermaid")
```

### Scheduling

Use the `schedule_pipeline` helper to run pipelines at fixed intervals:

```python
from dremioframe.orchestration import schedule_pipeline

# Run every 60 seconds
schedule_pipeline(pipeline, interval_seconds=60)
```

## Branching & Trigger Rules

You can control when a task runs based on the status of its upstream tasks using `trigger_rule`.

Available rules:
- `all_success` (Default): Runs only if all parents succeeded.
- `one_failed`: Runs if at least one parent failed. Useful for error handling/notifications.
- `all_done`: Runs regardless of parent status (Success, Failed, Skipped). Useful for cleanup.

### Example

```python
@task(name="process_data")
def process():
    # ...
    pass

@task(name="send_alert", trigger_rule="one_failed")
def alert():
    print("Something went wrong!")

t_proc = process()
t_alert = alert()

t_proc.set_downstream(t_alert)
```
