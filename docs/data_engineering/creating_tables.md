# Creating Tables

DremioFrame provides multiple ways to create tables in Dremio, from simple schema definitions to automatic schema inference from DataFrames.

## Quick Start

### Create an Empty Table with Schema Dictionary

The simplest way to create a table is with a schema dictionary mapping column names to SQL types:

```python
from dremioframe.client import DremioClient

client = DremioClient()

# Define schema
schema = {
    "id": "INTEGER",
    "name": "VARCHAR",
    "email": "VARCHAR",
    "created_at": "TIMESTAMP",
    "is_active": "BOOLEAN"
}

# Create the table
client.create_table("my_space.users", schema=schema)
```

### Create Table from DataFrame

You can create a table directly from a pandas or polars DataFrame. The schema is automatically inferred:

```python
import pandas as pd

# Create a DataFrame
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 92.1]
})

# Create table and insert data
client.create_table("my_space.scores", schema=df, insert_data=True)
```


### Upload File as Table

You can upload local files (CSV, JSON, Parquet, Excel, etc.) directly as new tables:

```python
# Upload a local CSV file
client.upload_file("data.csv", "my_space.my_data")

# Upload with explicit format
client.upload_file("data.xlsx", "my_space.excel_data", file_format="excel")
```

## Methods

### `client.create_table()`

The primary method for creating tables with full control over schema and data.

**Signature:**
```python
client.create_table(
    table_name: str,
    schema: Union[Dict[str, str], DataFrame, Table],
    data: Any = None,
    insert_data: bool = True
)
```

**Parameters:**
- `table_name`: Full table name (e.g., `"space.folder.table"`)
- `schema`: Either:
  - Dictionary mapping column names to SQL types
  - pandas DataFrame (schema inferred)
  - polars DataFrame (schema inferred)
  - pyarrow Table (schema inferred)
- `data`: Optional data to insert (only used with schema dict)
- `insert_data`: Whether to insert data when schema is a DataFrame/Table

### `builder.create()`

Create a table using CTAS (Create Table As Select) from a query or data:

```python
# CTAS from query
client.table("source_table") \\
    .filter("amount > 1000") \\
    .create("my_space.high_value_transactions")

# Create from DataFrame
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
client.table("temp").create("my_space.new_table", data=df)
```

### `builder.create_from_model()`

Create an empty table from a Pydantic model:

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str
    active: bool

client.table("temp").create_from_model("my_space.users", User)
```

## Supported Data Types

### Dremio SQL Types

When using schema dictionaries, you can specify these SQL types:

| SQL Type | Description | Example Values |
|----------|-------------|----------------|
| `INTEGER` | 32-bit integer | `-2147483648` to `2147483647` |
| `BIGINT` | 64-bit integer | Very large integers |
| `FLOAT` | Single precision | `3.14` |
| `DOUBLE` | Double precision | `3.141592653589793` |
| `BOOLEAN` | True/False | `TRUE`, `FALSE` |
| `VARCHAR` | Variable-length string | `"Hello World"` |
| `DATE` | Date only | `2024-01-15` |
| `TIMESTAMP` | Date and time | `2024-01-15 10:30:00` |
| `TIME` | Time only | `10:30:00` |
| `DECIMAL` | Fixed precision | `123.45` |
| `VARBINARY` | Binary data | Byte arrays |

> **Note**: Dremio does not support `TINYINT` or `SMALLINT`. Use `INTEGER` instead.

### Type Inference from DataFrames

When creating tables from DataFrames, types are automatically mapped:

| Python/Arrow Type | Dremio SQL Type |
|-------------------|-----------------|
| `int8`, `int16`, `int32` | `INTEGER` |
| `int64` | `BIGINT` |
| `float32` | `FLOAT` |
| `float64` | `DOUBLE` |
| `bool` | `BOOLEAN` |
| `str`, `string` | `VARCHAR` |
| `datetime64`, `timestamp` | `TIMESTAMP` |
| `date32`, `date64` | `DATE` |
| `time32`, `time64` | `TIME` |
| `decimal128`, `decimal256` | `DECIMAL` |

## Usage Patterns

### Pattern 1: Empty Table with Explicit Schema

Create an empty table to be populated later:

```python
schema = {
    "transaction_id": "BIGINT",
    "customer_id": "INTEGER",
    "amount": "DOUBLE",
    "transaction_date": "TIMESTAMP",
    "status": "VARCHAR"
}

client.create_table("finance.bronze.transactions", schema=schema)
```

### Pattern 2: Create and Populate from DataFrame

Create a table and immediately populate it with data:

```python
import pandas as pd

df = pd.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Widget A", "Widget B", "Widget C"],
    "price": [19.99, 29.99, 39.99],
    "in_stock": [True, False, True]
})

# Create table with data
client.create_table(
    "catalog.products",
    schema=df,
    insert_data=True
)
```

### Pattern 3: Create Empty Table from DataFrame Schema

Create an empty table using a DataFrame's schema without inserting data:

```python
# Create a sample DataFrame to define schema
schema_df = pd.DataFrame({
    "id": pd.Series([], dtype='int64'),
    "name": pd.Series([], dtype='str'),
    "value": pd.Series([], dtype='float64')
})

# Create empty table
client.create_table(
    "my_space.my_table",
    schema=schema_df,
    insert_data=False
)
```

### Pattern 4: Create with Schema Dict and Separate Data

Define schema explicitly, then insert data separately:

```python
# Define schema
schema = {
    "id": "INTEGER",
    "name": "VARCHAR",
    "score": "DOUBLE"
}

# Prepare data
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 92.1]
})

# Create table with schema and data
client.create_table(
    "my_space.scores",
    schema=schema,
    data=data,
    insert_data=True
)
```

### Pattern 5: CTAS (Create Table As Select)

Create a table from a query result:

```python
# Create table from filtered data
client.table("sales.transactions") \\
    .filter("amount > 1000") \\
    .filter("status = 'completed'") \\
    .create("sales.high_value_completed")

# Create aggregated table
client.table("sales.transactions") \\
    .group_by("customer_id") \\
    .agg(
        total_spent="SUM(amount)",
        transaction_count="COUNT(*)",
        avg_amount="AVG(amount)"
    ) \\
    .create("sales.customer_summary")
```

### Pattern 6: Create from Polars DataFrame

Works seamlessly with polars DataFrames:

```python
import polars as pl

df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 92.1]
})

client.create_table("my_space.scores", schema=df, insert_data=True)
```

### Pattern 7: Create from Arrow Table

Use pyarrow Tables directly:

```python
import pyarrow as pa

# Define schema
schema = pa.schema([
    ('id', pa.int64()),
    ('name', pa.string()),
    ('value', pa.float64())
])

# Create table with data
table = pa.table({
    'id': [1, 2, 3],
    'name': ['A', 'B', 'C'],
    'value': [1.1, 2.2, 3.3]
}, schema=schema)

client.create_table("my_space.data", schema=table, insert_data=True)
```

## Best Practices

### 1. Use Qualified Table Names

Always use fully qualified names (space.folder.table):

```python
# Good
client.create_table("finance.bronze.transactions", schema=schema)

# Avoid (may cause ambiguity)
client.create_table("transactions", schema=schema)
```

### 2. Choose Appropriate Data Types

Use the most appropriate type for your data:

```python
schema = {
    "id": "BIGINT",          # Use BIGINT for IDs that might grow large
    "amount": "DOUBLE",      # Use DOUBLE for currency/decimals
    "count": "INTEGER",      # Use INTEGER for counts
    "flag": "BOOLEAN",       # Use BOOLEAN for true/false
    "description": "VARCHAR" # Use VARCHAR for text
}
```

### 3. Validate Data Before Creating Tables

Ensure your DataFrame has the expected schema:

```python
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})

# Check dtypes
print(df.dtypes)

# Create table
client.create_table("my_space.users", schema=df, insert_data=True)
```

### 4. Handle Large Datasets

For large datasets, create the table first, then insert in batches:

```python
# Create empty table
schema = {"id": "INTEGER", "data": "VARCHAR"}
client.create_table("my_space.large_table", schema=schema)

# Insert in batches
for batch in large_data_batches:
    client.table("my_space.large_table").insert(
        "my_space.large_table",
        data=batch,
        batch_size=1000
    )
```

## Comparison of Methods

| Method | Use Case | Schema Source | Data Insertion |
|--------|----------|---------------|----------------|
| `create_table(schema=dict)` | Explicit schema definition | Manual | Optional |
| `create_table(schema=DataFrame)` | Infer from DataFrame | Automatic | Optional |
| `builder.create()` | CTAS from query | Query result | Automatic |
| `builder.create_from_model()` | Pydantic models | Pydantic | No |

## Troubleshooting

### Table Already Exists

```python
# Drop existing table first
client.execute("DROP TABLE IF EXISTS my_space.my_table")

# Then create
client.create_table("my_space.my_table", schema=schema)
```

### Type Mismatch Errors

Ensure your data types match the schema:

```python
# Convert types explicitly
df["id"] = df["id"].astype('int64')
df["score"] = df["score"].astype('float64')

client.create_table("my_space.data", schema=df, insert_data=True)
```

### Permission Errors

Ensure you have CREATE TABLE privileges on the target space:

```python
# Check your permissions or contact your Dremio administrator
```

## See Also

- [Ingestion API](ingestion.md) - For loading data from various sources
- [Builder API](builder.md) - For query building and CTAS
- [Pydantic Integration](pydantic_integration.md) - For schema validation
- [Data Types Reference](../reference/data_types.md) - Complete list of Dremio types
