# Local Caching with DataFusion

DremioFrame allows you to cache query results locally as Arrow Feather files and query them using the DataFusion python library. This is useful for iterative analysis where you don't want to repeatedly hit the Dremio engine for the same data.

## Usage

Use the `cache()` method on a `DremioBuilder` object.

```python
# Cache the result of a query for 5 minutes (300 seconds)
# If the cache file exists and is younger than 5 minutes, it will be used.
# Otherwise, the query is executed on Dremio and the result is saved.
local_df = client.table("source.table") \
    .filter("col > 10") \
    .cache("my_cache", ttl_seconds=300)

# local_df is a LocalBuilder backed by DataFusion
# You can continue chaining methods
result = local_df.filter("col < 50") \
    .group_by("category") \
    .agg(avg_val="AVG(val)") \
    .collect()

print(result)
```

## Features

- **TTL (Time-To-Live)**: Automatically invalidates cache if it's too old.
- **DataFusion Engine**: Executes SQL locally on the cached Arrow file, providing fast performance without network overhead.
- **Seamless Integration**: `LocalBuilder` mimics the `DremioBuilder` API for `select`, `filter`, `group_by`, `agg`, `order_by`, `limit`, and `sql`.

## API

### `cache(name, ttl_seconds=None, folder=".cache")`

- `name`: Name of the cache file (saved as `{folder}/{name}.feather`).
- `ttl_seconds`: Expiration time in seconds. If `None`, cache never expires (unless manually deleted).
- `folder`: Directory to store cache files. Defaults to `.cache`.
