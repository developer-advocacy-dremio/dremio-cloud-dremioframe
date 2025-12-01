# Orchestration Best Practices

This guide provides recommendations and patterns for building robust data pipelines using `dremioframe.orchestration`.

## 1. Organizing Your Tasks

### Use the `@task` Decorator
The decorator syntax is cleaner and keeps your code readable.

```python
from dremioframe.orchestration import task

@task(name="extract_data")
def extract():
    ...
```

### Keep Tasks Atomic
Each task should do one thing well. This makes debugging easier and allows for better retry granularity.

**Bad:**
```python
@task(name="do_everything")
def run():
    # Extract
    # Transform
    # Load
    # Email
```

**Good:**
```python
@task(name="extract")
def extract(): ...

@task(name="transform")
def transform(): ...

@task(name="load")
def load(): ...
```

## 2. Managing Dependencies

### Linear Chains
For simple sequences, chain the calls:

```python
t1.set_downstream(t2).set_downstream(t3)
```

### Fan-Out / Fan-In
Run multiple tasks in parallel and then aggregate results.

```python
extract_users = extract("users")
extract_orders = extract("orders")
extract_products = extract("products")

consolidate = consolidate_data()

# Fan-in
extract_users.set_downstream(consolidate)
extract_orders.set_downstream(consolidate)
extract_products.set_downstream(consolidate)
```

## 3. Handling Failures

### Use Retries for Transient Errors
Network blips happen. Always add retries to tasks that interact with external systems (Dremio, S3, APIs).

```python
@task(name="query_dremio", retries=3, retry_delay=2.0)
def query():
    ...
```

### Use Branching for Alerts
Don't let a failure go unnoticed. Use the `one_failed` trigger rule to send notifications.

```python
@task(name="alert_slack", trigger_rule="one_failed")
def alert(context=None):
    # Send message to Slack
    pass

critical_task.set_downstream(alert)
```

### Use `all_done` for Cleanup
Ensure temporary resources are cleaned up even if the pipeline fails.

```python
@task(name="cleanup_tmp", trigger_rule="all_done")
def cleanup():
    # Delete tmp files
    pass
```

## 4. Data Passing (Context)

### Return Small Metadata, Not Big Data
Do not pass large DataFrames between tasks via return values. The context is kept in memory.
Instead, pass **references** (e.g., table names, S3 paths, file paths).

**Bad:**
```python
@task
def get_data():
    return huge_dataframe # Don't do this
```

**Good:**
```python
@task
def get_data():
    df = ...
    df.to_parquet("s3://bucket/data.parquet")
    return "s3://bucket/data.parquet"

@task
def process(context=None):
    path = context.get("get_data")
    # Load from path
```

## 5. Project Structure

Organize your pipelines into a dedicated directory.

```
my_project/
├── pipelines/
│   ├── __init__.py
│   ├── daily_etl.py
│   └── weekly_report.py
├── tasks/
│   ├── __init__.py
│   ├── common.py
│   └── dremio_tasks.py
└── main.py
```

## 6. Testing

Write unit tests for your tasks by calling the underlying functions directly (if possible) or checking the Task object.
Use `dremioframe`'s testing utilities to mock Dremio responses.
