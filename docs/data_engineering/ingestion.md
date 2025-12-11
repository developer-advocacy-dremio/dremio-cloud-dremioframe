# Ingestion Overview

DremioFrame provides multiple ways to ingest data into Dremio, ranging from simple API calls to complex file system and database integrations.

## 1. API Ingestion

Ingest data directly from REST APIs. This method is built into the main client.

**Method:** `client.ingest_api(...)`

```python
client.ingest_api(
    url="https://api.example.com/users",
    table_name="raw_users",
    mode="replace" # 'replace', 'append', or 'merge'
)
```

[Read more about API Ingestion strategies (Modes, Auth, Batching)](ingestion_patterns.md)

## 2. File Upload

Upload local files (CSV, JSON, Parquet, Excel, etc.) directly to Dremio as tables.

**Method:** `client.upload_file(...)`

```python
client.upload_file("data/sales.csv", "space.folder.sales_table")
```

[Read the full File Upload guide](file_upload.md)

## 3. Ingestion Modules

Advanced ingestion capabilities are grouped under the `client.ingest` namespace.

### DLT (Data Load Tool)

Integration with the `dlt` library for robust pipelines.

**Method:** `client.ingest.dlt(...)`

```python
data = [{"id": 1, "name": "Alice"}]
client.ingest.dlt(data, "my_dlt_table")
```

[Read the DLT Integration guide](dlt_integration.md)

### Database Ingestion

Ingest query results from other databases (Postgres, MySQL, etc.) using JDBC/ODBC connectors via `connectorx` or `sqlalchemy`.

**Method:** `client.ingest.database(...)`

```python
client.ingest.database(
    connection_string="postgresql://user:pass@localhost/db",
    query="SELECT * FROM users",
    table_name="postgres_users"
)
```

[Read the Database Ingestion guide](database_ingestion.md)

### File System Ingestion

Ingest multiple files from a local directory or glob pattern.

**Method:** `client.ingest.files(...)`

```python
client.ingest.files("data/*.parquet", "my_dataset")
```

[Read the File System Ingestion guide](file_system_ingestion.md)
