# Orchestration Tasks

DremioFrame provides a set of general-purpose tasks to extend your pipelines beyond Dremio operations.

## General Tasks

Import these from `dremioframe.orchestration.tasks.general`.

### HttpTask

Performs HTTP requests. Useful for triggering webhooks or fetching external data.

```python
from dremioframe.orchestration.tasks.general import HttpTask

task = HttpTask(
    name="trigger_webhook",
    url="https://api.example.com/webhook",
    method="POST",
    json_data={"status": "pipeline_started"}
)
```

### EmailTask

Sends emails via SMTP. Useful for notifications.

```python
from dremioframe.orchestration.tasks.general import EmailTask

task = EmailTask(
    name="send_alert",
    subject="Pipeline Failed",
    body="The pipeline encountered an error.",
    to_addr="admin@example.com",
    smtp_server="smtp.example.com",
    smtp_port=587,
    use_tls=True,
    username="user",
    password="password"
)
```

### ShellTask

Executes arbitrary shell commands.

```python
from dremioframe.orchestration.tasks.general import ShellTask

task = ShellTask(
    name="run_dbt",
    command="dbt run",
    cwd="/path/to/dbt/project",
    env={"DBT_PROFILES_DIR": "."}
)
```

### S3Task

Interacts with AWS S3. Requires `boto3`.

**Requirements:**
```bash
pip install "dremioframe[s3]"
```

**Usage:**
```python
from dremioframe.orchestration.tasks.general import S3Task

# Upload
upload = S3Task(
    name="upload_report",
    operation="upload_file",
    bucket="my-bucket",
    key="reports/daily.csv",
    local_path="/tmp/daily.csv"
)

# Download
download = S3Task(
    name="download_config",
    operation="download_file",
    bucket="my-bucket",
    key="config/settings.json",
    local_path="/app/settings.json"
)
```
