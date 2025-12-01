# Airflow Integration

DremioFrame provides native integration with Apache Airflow, allowing you to orchestrate Dremio workflows within your Airflow DAGs.

## Installation

Install DremioFrame with Airflow support:

```bash
pip install "dremioframe[airflow]"
```

This will install `apache-airflow` as a dependency.

## Components

### DremioHook

The `DremioHook` wraps the `DremioClient` and integrates with Airflow's connection management system.

#### Configuration

Create a Dremio connection in Airflow:

**Via Airflow UI:**
1. Navigate to Admin → Connections
2. Add a new connection with the following details:
   - **Connection ID**: `dremio_default` (or custom name)
   - **Connection Type**: `Dremio` (or `Generic` if custom type not available)
   - **Host**: `data.dremio.cloud` (or your Dremio hostname)
   - **Port**: `443` (or your port)
   - **Login**: Username (for Dremio Software with username/password auth)
   - **Password**: Password or PAT
   - **Extra**: JSON with additional config

**Extra JSON Fields:**
```json
{
  "pat": "your_personal_access_token",
  "project_id": "your_project_id",
  "tls": true,
  "disable_certificate_verification": false
}
```

**Via Environment Variable:**
```bash
export AIRFLOW_CONN_DREMIO_DEFAULT='{"conn_type": "dremio", "host": "data.dremio.cloud", "port": 443, "extra": {"pat": "YOUR_PAT", "project_id": "YOUR_PROJECT_ID"}}'
```

#### Usage

```python
from dremioframe.airflow import DremioHook

hook = DremioHook(dremio_conn_id="dremio_default")
client = hook.get_conn()

# Execute SQL
df = hook.get_pandas_df("SELECT * FROM my_table LIMIT 10")

# Get records as list of dicts
records = hook.get_records("SELECT * FROM my_table LIMIT 10")
```

### DremioSQLOperator

Execute SQL queries in Dremio as part of your Airflow DAG.

#### Parameters

- `sql` (str): The SQL query to execute. Supports Jinja templating.
- `dremio_conn_id` (str): Connection ID. Default: `"dremio_default"`.
- `return_result` (bool): Whether to return results as XCom. Default: `False`.

#### Example

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dremioframe.airflow import DremioSQLOperator

default_args = {
    'owner': 'data_team',
    'start_date': days_ago(1),
}

with DAG('dremio_etl', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Create a table
    create_table = DremioSQLOperator(
        task_id='create_staging_table',
        sql='''
            CREATE TABLE IF NOT EXISTS staging.daily_metrics AS
            SELECT 
                DATE_TRUNC('day', event_time) as date,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM raw.events
            WHERE DATE(event_time) = CURRENT_DATE - INTERVAL '1' DAY
            GROUP BY 1
        '''
    )
    
    # Run aggregation
    aggregate_data = DremioSQLOperator(
        task_id='aggregate_metrics',
        sql='''
            INSERT INTO analytics.daily_summary
            SELECT * FROM staging.daily_metrics
        '''
    )
    
    # Optimize table
    optimize_table = DremioSQLOperator(
        task_id='optimize_table',
        sql='OPTIMIZE TABLE analytics.daily_summary'
    )
    
    create_table >> aggregate_data >> optimize_table
```

#### Templating

The `sql` parameter supports Jinja templating:

```python
DremioSQLOperator(
    task_id='process_partition',
    sql='''
        SELECT * FROM events
        WHERE date = '{{ ds }}'  -- Airflow execution date
    '''
)
```

### DremioDataQualityOperator

Run data quality checks on Dremio tables.

#### Parameters

- `table_name` (str): The table to check. Supports Jinja templating.
- `checks` (list): List of check definitions.
- `dremio_conn_id` (str): Connection ID. Default: `"dremio_default"`.

#### Check Types

- `not_null`: Check that a column has no NULL values.
- `unique`: Check that a column has only unique values.
- `row_count`: Check row count with a condition.
- `values_in`: Check that column values are in a specified list.

#### Example

```python
from dremioframe.airflow import DremioDataQualityOperator

dq_check = DremioDataQualityOperator(
    task_id='validate_daily_metrics',
    table_name='staging.daily_metrics',
    checks=[
        {
            "type": "not_null",
            "column": "date"
        },
        {
            "type": "row_count",
            "expr": "event_count > 0",
            "value": 1,
            "op": "ge"  # greater than or equal
        },
        {
            "type": "unique",
            "column": "date"
        }
    ]
)
```

If any check fails, the operator will raise a `ValueError` and fail the task.

## Complete DAG Example

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dremioframe.airflow import DremioSQLOperator, DremioDataQualityOperator

default_args = {
    'owner': 'data_team',
    'start_date': days_ago(1),
    'retries': 2,
}

with DAG(
    'dremio_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False
) as dag:
    
    # Extract
    extract = DremioSQLOperator(
        task_id='extract_raw_data',
        sql='''
            CREATE TABLE staging.raw_events_{{ ds_nodash }} AS
            SELECT * FROM raw.events
            WHERE DATE(event_time) = '{{ ds }}'
        '''
    )
    
    # Transform
    transform = DremioSQLOperator(
        task_id='transform_data',
        sql='''
            CREATE TABLE staging.metrics_{{ ds_nodash }} AS
            SELECT 
                user_id,
                COUNT(*) as event_count,
                MAX(event_time) as last_event
            FROM staging.raw_events_{{ ds_nodash }}
            GROUP BY user_id
        '''
    )
    
    # Data Quality
    dq_check = DremioDataQualityOperator(
        task_id='validate_metrics',
        table_name='staging.metrics_{{ ds_nodash }}',
        checks=[
            {"type": "not_null", "column": "user_id"},
            {"type": "row_count", "expr": "event_count > 0", "value": 1, "op": "ge"}
        ]
    )
    
    # Load
    load = DremioSQLOperator(
        task_id='load_to_analytics',
        sql='''
            INSERT INTO analytics.user_metrics
            SELECT '{{ ds }}' as date, *
            FROM staging.metrics_{{ ds_nodash }}
        '''
    )
    
    # Cleanup
    cleanup = DremioSQLOperator(
        task_id='cleanup_staging',
        sql='''
            DROP TABLE staging.raw_events_{{ ds_nodash }};
            DROP TABLE staging.metrics_{{ ds_nodash }};
        '''
    )
    
    extract >> transform >> dq_check >> load >> cleanup
```

## Best Practices

### 1. Use XComs Sparingly

Avoid returning large datasets via XCom:

```python
# Bad - returns large dataset
DremioSQLOperator(
    task_id='get_data',
    sql='SELECT * FROM large_table',
    return_result=True  # Avoid this for large results
)

# Good - process in Dremio, only return metadata
DremioSQLOperator(
    task_id='process_data',
    sql='CREATE TABLE result AS SELECT * FROM large_table WHERE ...'
)
```

### 2. Leverage Templating

Use Airflow's templating for dynamic queries:

```python
DremioSQLOperator(
    task_id='partition_process',
    sql='''
        OPTIMIZE TABLE my_table
        WHERE partition_date = '{{ ds }}'
    '''
)
```

### 3. Idempotent Operations

Make your SQL idempotent for safe retries:

```python
# Use CREATE TABLE IF NOT EXISTS
DremioSQLOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS ...'
)

# Or use MERGE for upserts
DremioSQLOperator(
    task_id='upsert_data',
    sql='''
        MERGE INTO target USING source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET ...
        WHEN NOT MATCHED THEN INSERT ...
    '''
)
```

### 4. Connection Pooling

Reuse connections within a DAG by using the same `dremio_conn_id`.

### 5. Error Handling

Use Airflow's built-in retry mechanism:

```python
DremioSQLOperator(
    task_id='flaky_operation',
    sql='...',
    retries=3,
    retry_delay=timedelta(minutes=5)
)
```

## Comparison with Native Orchestration

DremioFrame includes its own lightweight orchestration engine. Here's when to use each:

| Use Airflow When... | Use DremioFrame Orchestration When... |
|---------------------|---------------------------------------|
| You already have Airflow infrastructure | You want a lightweight, standalone solution |
| You need complex scheduling (cron, sensors) | You need simple task dependencies |
| You integrate with many other systems | You only work with Dremio |
| You need enterprise features (RBAC, audit logs) | You want minimal dependencies |
| You have a dedicated data engineering team | You're a data analyst or scientist |

You can also use both: develop pipelines with DremioFrame orchestration, then migrate to Airflow for production.

## Troubleshooting

### Connection Issues

If you see `ModuleNotFoundError: No module named 'dremioframe'`:
- Ensure DremioFrame is installed in the Airflow environment
- Check `pip list | grep dremioframe`

### Authentication Errors

If you see authentication failures:
- Verify PAT is valid: `curl -H "Authorization: Bearer YOUR_PAT" https://api.dremio.cloud/v0/projects`
- Check connection configuration in Airflow UI
- Ensure `project_id` is set for Dremio Cloud

### Query Timeouts

For long-running queries:
- Increase Airflow task timeout: `execution_timeout=timedelta(hours=2)`
- Consider breaking into smaller tasks
- Use Dremio reflections to accelerate queries

## See Also

- [Orchestration Overview](../orchestration/overview.md)
- [Data Quality](../data_quality.md)
- [Administration](../admin_governance/admin.md)
