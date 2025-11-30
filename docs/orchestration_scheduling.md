# Orchestration Scheduling

`dremioframe` supports scheduling pipelines to run at fixed intervals or according to a Cron expression.

## Cron Scheduling

You can use standard 5-field cron expressions to schedule your pipelines.

Format: `Minute Hour DayOfMonth Month DayOfWeek`

### Example

```python
from dremioframe.orchestration import schedule_pipeline, Pipeline

pipeline = Pipeline("my_scheduled_pipeline")
# ... add tasks ...

# Run every day at 9:30 AM
schedule_pipeline(pipeline, cron="30 9 * * *")

# Run every Monday at 8:00 AM
schedule_pipeline(pipeline, cron="0 8 * * 1")

# Run every 15 minutes
schedule_pipeline(pipeline, cron="*/15 * * * *")
```

## Interval Scheduling

You can also schedule by a simple interval in seconds.

```python
# Run every 60 seconds
schedule_pipeline(pipeline, interval_seconds=60)
```
