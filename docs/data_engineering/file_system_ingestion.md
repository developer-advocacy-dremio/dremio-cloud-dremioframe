# File System Ingestion

DremioFrame provides a convenient way to ingest multiple files from your local filesystem or network drives using glob patterns.

## Installation

No additional dependencies required - this feature uses the core DremioFrame installation.

## Usage

The integration is exposed via `client.ingest.files()`.

### Example: Loading Multiple Parquet Files

```python
from dremioframe.client import DremioClient

client = DremioClient()

# Ingest all parquet files in a directory
client.ingest.files(
    pattern="data/sales_*.parquet",
    table_name='"my_space"."my_folder"."sales"',
    write_disposition="replace"
)
```

### Example: Recursive Directory Scan

```python
# Ingest all CSV files in directory tree
client.ingest.files(
    pattern="data/**/*.csv",
    table_name='"my_space"."logs"."all_logs"',
    file_format="csv",
    recursive=True,
    write_disposition="append"
)
```

## Parameters

- **pattern**: Glob pattern (e.g., `"data/*.parquet"`, `"sales_2024_*.csv"`).
- **table_name**: The target table name in Dremio.
- **file_format**: File format (`'parquet'`, `'csv'`, `'json'`). Auto-detected from extension if not specified.
- **write_disposition**: 
    - `'replace'`: Drop table if exists and create new.
    - `'append'`: Append data to existing table.
- **recursive**: If `True`, enables recursive glob (`**` pattern).

## Supported File Formats

- **Parquet** (`.parquet`)
- **CSV** (`.csv`)
- **JSON** (`.json`, `.jsonl`, `.ndjson`)

## How It Works

1. Finds all files matching the glob pattern
2. Reads each file into an Arrow Table
3. Concatenates all tables into a single table
4. Uses the **staging method** (Parquet upload) for efficient bulk loading
5. Creates or appends to the target table in Dremio

## Performance

File system ingestion automatically uses the **staging method** for bulk loading, providing excellent performance even with large datasets:

- Handles 100+ files efficiently
- Supports files with millions of rows
- Minimal memory footprint (streaming read)

## Use Cases

- **Data Lake Ingestion**: Load partitioned datasets from S3/HDFS mounted locally
- **Batch Processing**: Ingest daily/hourly file drops
- **Migration**: Bulk load historical data from file archives
- **Development**: Quick data loading from local test files
