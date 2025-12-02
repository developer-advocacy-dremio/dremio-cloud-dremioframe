# Bulk Loading Optimization

For large datasets (10,000+ rows), using the default `VALUES` clause method can be slow and may hit SQL statement size limits. DremioFrame provides a **staging method** that dramatically improves performance by using Parquet files as an intermediate format.

## Usage

Both `create()` and `insert()` methods support a `method` parameter:

```python
from dremioframe.client import DremioClient
import pandas as pd

client = DremioClient()

# Create large dataset
data = pd.DataFrame({
    "id": range(100000),
    "name": [f"user_{i}" for i in range(100000)],
    "value": range(100000)
})

# Use staging method for fast bulk load
client.table('"my_space"."my_folder"."large_table"').create(
    '"my_space"."my_folder"."large_table"',
    data=data,
    method="staging"  # Much faster than default "values"
)
```

## How It Works

### Values Method (Default)

```python
method="values"  # Default
```

- Generates SQL `INSERT INTO ... VALUES (...)` statements
- Good for small datasets (< 10,000 rows)
- Simple and straightforward
- Can hit SQL statement size limits with large data

### Staging Method (Recommended for Large Data)

```python
method="staging"
```

**For `create()`:**
1. Writes data to a temporary local Parquet file
2. Uploads the Parquet file to Dremio (creates the table)
3. Cleans up the temporary file

**For `insert()`:**
1. Writes data to a temporary local Parquet file
2. Uploads to a temporary staging table in Dremio
3. Executes `INSERT INTO target SELECT * FROM staging_table`
4. Drops the staging table
5. Cleans up the temporary file

## Performance Comparison

| Rows    | Values Method | Staging Method | Speedup |
|---------|---------------|----------------|---------|
| 1,000   | ~2s           | ~3s            | 0.67x   |
| 10,000  | ~20s          | ~5s            | 4x      |
| 100,000 | Fails*        | ~15s           | ∞       |

*SQL statement size limit exceeded

## When to Use Staging

Use `method="staging"` when:
- Loading more than 10,000 rows
- Experiencing slow `INSERT` performance
- Hitting SQL statement size limits
- Working with wide tables (many columns)

Use `method="values"` (default) when:
- Loading small datasets (< 1,000 rows)
- Simplicity is preferred over performance
- You don't have write access to create temporary tables
