# Database Ingestion

DremioFrame provides a standardized way to ingest data from any SQL database (PostgreSQL, MySQL, SQLite, Oracle, etc.) into Dremio.

## Installation

To use the database ingestion feature, you must install the optional dependencies:

```bash
pip install dremioframe[database]
```

This installs `connectorx` (for high-performance loading) and `sqlalchemy` (for broad compatibility).

## Usage

The integration is exposed via `client.ingest.database()`.

### Example: Loading from PostgreSQL

```python
from dremioframe.client import DremioClient

client = DremioClient()

# Connection string (URI)
db_uri = "postgresql://user:password@localhost:5432/mydb"

# Ingest query results into Dremio
client.ingest.database(
    connection_string=db_uri,
    query="SELECT * FROM users WHERE active = true",
    table_name='"my_space"."my_folder"."users"',
    write_disposition="replace",
    backend="connectorx" # Default, faster
)
```

### Parameters

- **connection_string**: Database connection URI (e.g., `postgresql://...`, `mysql://...`).
- **query**: SQL query to execute on the source database.
- **table_name**: The target table name in Dremio.
- **write_disposition**: `'replace'` or `'append'`.
- **backend**:
    - `'connectorx'` (default): Extremely fast, written in Rust. Supports Postgres, MySQL, SQLite, Redshift, Clickhouse, SQL Server.
    - `'sqlalchemy'`: Uses standard SQLAlchemy engines. Slower but supports any database with a Python driver.
- **batch_size**: (Only for `sqlalchemy` backend) Number of records to process per batch. Useful for large datasets to avoid memory issues.

### Performance Tips

- Use `backend="connectorx"` whenever possible for significantly faster load times.
- For very large tables with `sqlalchemy`, set a `batch_size` (e.g., 50,000) to stream data instead of loading it all into memory.
