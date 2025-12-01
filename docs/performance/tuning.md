# Performance Tuning Guide

Optimizing `dremioframe` applications involves tuning both the client-side Python code and the server-side Dremio execution.

## 1. Arrow Flight Optimization

DremioFrame uses Apache Arrow Flight for high-performance data transfer.

### Batch Sizes

When fetching large datasets using `collect()`, the data is streamed in chunks.
*   **Default**: Dremio controls the chunk size.
*   **Optimization**: Ensure your network has high throughput. Flight is bandwidth-bound.

When **writing** data (`insert`, `create`), `dremioframe` splits data into batches to avoid hitting message size limits (usually 2GB, but practically smaller).

```python
# Default batch size is often safe, but for very wide tables, reduce it.
client.table("target").insert("target", data=df, batch_size=5000)
```

### Compression

Flight supports compression (LZ4/ZSTD). DremioFrame negotiates this automatically. Ensure your client machine has CPU cycles to spare for decompression.

## 2. Client-Side vs. Server-Side Processing

Always push filtering and aggregation to Dremio (Server-Side) before collecting data to Python (Client-Side).

**Bad Pattern (Client-Side Filtering):**
```python
# Fetches ALL data, then filters in Python
df = client.table("sales").collect()
filtered_df = df.filter(pl.col("amount") > 100)
```

**Good Pattern (Server-Side Filtering):**
```python
# Filters in Dremio, fetches only matching rows
df = client.table("sales").filter("amount > 100").collect()
```

## 3. Parallelism

### Pipeline Parallelism
Use the `orchestration` module to run independent tasks in parallel.

```python
pipeline = Pipeline("etl", max_workers=4)
```

### Async Client
For high-concurrency applications (e.g., a web app backend), use `AsyncDremioClient` to avoid blocking the main thread while waiting for Dremio.

```python
async with AsyncDremioClient() as client:
    result = await client.query("SELECT * FROM large_table")
```

## 4. Caching

If you query the same dataset multiple times in a script, cache it locally.

```python
# Cache the result of a heavy query to a local Parquet file
cached_df = client.table("heavy_view").cache("local_cache_name", ttl_seconds=600)

# Subsequent operations use the local file (via DuckDB/DataFusion)
cached_df.filter("col1 = 1").show()
```
