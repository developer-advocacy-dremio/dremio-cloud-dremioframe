# Async Client

`dremioframe` provides an asynchronous client for high-concurrency applications.

## Usage

The `AsyncDremioClient` is designed to be used as an async context manager.

```python
import asyncio
from dremioframe.async_client import AsyncDremioClient

async def main():
    async with AsyncDremioClient(pat="my-pat") as client:
        # Get catalog item
        item = await client.get_catalog_item("dataset-id")
        print(item)
        
        # Execute SQL (REST API)
        job = await client.execute_sql("SELECT 1")
        print(job)

if __name__ == "__main__":
    asyncio.run(main())
```

## Methods

- `get_catalog_item(id)`: Get catalog item by ID.
- `get_catalog_by_path(path)`: Get catalog item by path list.
- `execute_sql(sql)`: Submit a SQL job via REST API.
- `get_job_status(job_id)`: Check job status.
