# Orchestration API Reference

## Pipeline

::: dremioframe.orchestration.pipeline.Pipeline
    options:
      show_root_heading: true

## Tasks

::: dremioframe.orchestration.task.Task
    options:
      show_root_heading: true

### Dremio Tasks

::: dremioframe.orchestration.tasks.dremio_tasks.DremioQueryTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.tasks.builder_task.DremioBuilderTask
    options:
      show_root_heading: true

### General Tasks

::: dremioframe.orchestration.tasks.general.HttpTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.tasks.general.EmailTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.tasks.general.ShellTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.tasks.general.S3Task
    options:
      show_root_heading: true

### Extension Tasks

::: dremioframe.orchestration.tasks.dbt_task.DbtTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.tasks.dq_task.DataQualityTask
    options:
      show_root_heading: true

### Iceberg Tasks

::: dremioframe.orchestration.iceberg_tasks.OptimizeTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.iceberg_tasks.VacuumTask
    options:
      show_root_heading: true

::: dremioframe.orchestration.iceberg_tasks.ExpireSnapshotsTask
    options:
      show_root_heading: true

### Reflection Tasks

::: dremioframe.orchestration.reflection_tasks.RefreshReflectionTask
    options:
      show_root_heading: true

## Sensors

::: dremioframe.orchestration.sensors.SqlSensor
    options:
      show_root_heading: true

::: dremioframe.orchestration.sensors.FileSensor
    options:
      show_root_heading: true

## Executors

::: dremioframe.orchestration.executors.LocalExecutor
    options:
      show_root_heading: true

::: dremioframe.orchestration.executors.CeleryExecutor
    options:
      show_root_heading: true

## Scheduling

::: dremioframe.orchestration.scheduling.schedule_pipeline
    options:
      show_root_heading: true

## Backends

::: dremioframe.orchestration.backend.BaseBackend
    options:
      show_root_heading: true

::: dremioframe.orchestration.backend.PostgresBackend
    options:
      show_root_heading: true

::: dremioframe.orchestration.backend.MySQLBackend
    options:
      show_root_heading: true

::: dremioframe.orchestration.backend.SQLiteBackend
    options:
      show_root_heading: true

::: dremioframe.orchestration.backend.InMemoryBackend
    options:
      show_root_heading: true
