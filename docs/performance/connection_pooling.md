# Connection Pooling

DremioFrame includes a `ConnectionPool` to manage and reuse `DremioClient` instances, which is essential for high-concurrency applications or long-running services.

## ConnectionPool

The `ConnectionPool` manages a thread-safe queue of clients.

### Initialization

```python
from dremioframe.connection_pool import ConnectionPool

# Create a pool with 5 connections
pool = ConnectionPool(
    max_size=5,
    timeout=30,
    # DremioClient arguments
    pat="YOUR_PAT",
    project_id="YOUR_PROJECT_ID"
)
```

### Using the Context Manager

The recommended way to use the pool is via the context manager, which ensures connections are returned to the pool even if errors occur.

```python
with pool.client() as client:
    # Use client as normal
    df = client.sql("SELECT * FROM sys.version").collect()
    print(df)
```

### Manual Management

You can also manually get and release clients.

```python
try:
    client = pool.get_client()
    # Use client...
finally:
    pool.release_client(client)
```

### Configuration

- **max_size**: Maximum number of connections to create.
- **timeout**: Seconds to wait for a connection if the pool is empty and at max size. Raises `TimeoutError` if exceeded.
- **client_kwargs**: Arguments passed to `DremioClient` constructor (e.g., `pat`, `username`, `password`, `flight_endpoint`).
