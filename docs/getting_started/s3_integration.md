# S3 Integration

DremioFrame provides seamless integration with Amazon S3 through the optional `s3` dependency. This allows you to perform file operations directly within your orchestration pipelines, such as uploading data files before ingestion or downloading results.

## Installation

To use S3 features, you must install the `s3` optional dependency group, which includes `boto3`.

```bash
pip install "dremioframe[s3]"
```

## S3Task

The primary way to interact with S3 is through the `S3Task` in the orchestration module. This task wraps `boto3` operations into a reusable pipeline component.

### Supported Operations

- **`upload_file`**: Upload a local file to an S3 bucket.
- **`download_file`**: Download a file from an S3 bucket to a local path.

### Configuration

You can configure credentials directly in the task or rely on environment variables (recommended).

**Environment Variables:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

### Examples

#### Uploading a File

```python
from dremioframe.orchestration import Pipeline
from dremioframe.orchestration.tasks import S3Task

# Define the task
upload_task = S3Task(
    name="upload_csv",
    operation="upload_file",
    bucket="my-datalake-bucket",
    key="raw/data.csv",
    local_path="./data/local_data.csv"
)

# Create and run pipeline
pipeline = Pipeline("s3_ingest")
pipeline.add_task(upload_task)
pipeline.run()
```

#### Downloading a File

```python
download_task = S3Task(
    name="download_report",
    operation="download_file",
    bucket="my-reports-bucket",
    key="monthly/report.pdf",
    local_path="./downloads/report.pdf"
)
```

#### Using Custom Credentials

```python
custom_s3_task = S3Task(
    name="upload_secure",
    operation="upload_file",
    bucket="secure-bucket",
    key="data.csv",
    local_path="./data.csv",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="SECRET...",
    region_name="us-west-2"
)
```

#### Using MinIO or S3-Compatible Storage

You can connect to S3-compatible storage like MinIO by providing an `endpoint_url`.

```python
minio_task = S3Task(
    name="upload_minio",
    operation="upload_file",
    bucket="test-bucket",
    key="test.csv",
    local_path="./test.csv",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)
```
