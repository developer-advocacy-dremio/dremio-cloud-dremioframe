# API Ingestion

DremioFrame provides a utility to easily ingest data from REST APIs into Dremio tables.

## Usage

```python
client.ingest_api(
    url="https://api.example.com/users",
    table_name="raw_users",
    mode="replace"
)
```

## Modes

### Replace
Drops the target table if it exists and creates a new one with the fetched data.

```python
client.ingest_api(url="...", table_name="users", mode="replace")
```

### Append (Incremental)
Appends new records to the existing table. If `pk` (Primary Key) is provided, it queries the target table for the maximum value of `pk` and only inserts records from the API where `pk > max_pk`.

```python
client.ingest_api(
    url="...", 
    table_name="users", 
    mode="append", 
    pk="id" # Only ingest users with id > max(id) in target
)
```

### Merge (Upsert)
Performs a MERGE operation. It writes the API data to a temporary staging table, then merges it into the target table based on the `pk`.

```python
client.ingest_api(
    url="...", 
    table_name="users", 
    mode="merge", 
    pk="id" # Update existing IDs, insert new IDs
)
```

## Advanced Options

- **headers**: Dict of HTTP headers (e.g., for authentication).
- **json_path**: Dot-notation path to extract the list of records from the JSON response (e.g., `data.items`).
- **batch_size**: Number of records to insert per batch.

```python
client.ingest_api(
    url="https://api.example.com/data",
    table_name="my_table",
    headers={"Authorization": "Bearer token"},
    json_path="response.results",
    batch_size=1000
)
```
