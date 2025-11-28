# Dataframe Builder

The `DremioBuilder` provides a fluent interface for querying data, similar to Ibis or PySpark.

## Getting a Builder

Start by selecting a table from the client:

```python
from dremioframe.client import DremioClient

client = DremioClient()
df = client.table("Samples.samples.dremio.com.zips.json")
```

## Querying Data

You can chain methods to build a query:

```python
result = (
    df.select("city", "state", "pop")
      .filter("state = 'MA'")
      .limit(10)
      .collect()
)

print(result)
```

### Methods

- `select(*cols)`: Select specific columns.
- `mutate(**kwargs)`: Add calculated columns. Example: `.mutate(total="price * quantity")`
- `filter(condition)`: Add a WHERE clause.
- `limit(n)`: Limit the number of rows.
- `collect(library='polars')`: Execute the query and return a DataFrame. Supported libraries: `polars` (default), `pandas`.
- `show(n=20)`: Print the first `n` rows.

## DML Operations

You can also perform Data Manipulation Language (DML) operations:

```python
# Create Table As Select (CTAS)
df.filter("state = 'NY'").create("NewYorkZips")

# Insert into existing table from query
df.filter("state = 'NJ'").insert("NewYorkZips")

# Insert from Pandas DataFrame or Arrow Table
import pandas as pd
data = pd.DataFrame({"city": ["New City"], "state": ["NY"]})
client.table("NewYorkZips").insert("NewYorkZips", data=data)

# Update rows
client.table("NewYorkZips").filter("city = 'New York'").update({"pop": 9000000})

# Delete rows
client.table("NewYorkZips").filter("pop < 1000").delete()

## Merge (Upsert)

You can perform `MERGE INTO` operations to upsert data.

```python
# Upsert from a DataFrame
client.table("target").merge(
    target_table="target",
    on="id",
    matched_update={"val": "source.val"},
    not_matched_insert={"id": "source.id", "val": "source.val"},
    data=df_upsert
)
```

## Batching

For `insert` and `merge` operations with in-memory data (Arrow Table or Pandas DataFrame), you can specify a `batch_size` to split the data into multiple chunks. This is useful for large datasets to avoid hitting query size limits.

```python
# Insert in batches of 1000 rows
client.table("target").insert("target", data=large_df, batch_size=1000)
```

## Data Quality Checks

You can run data quality checks on a builder instance. These checks execute queries to verify assumptions about the data.

```python
# Check that 'city' is never NULL
df.quality.expect_not_null("city")

# Check that 'zip' is unique
df.quality.expect_unique("zip")

# Check that 'state' is one of the allowed values
df.quality.expect_values_in("state", ["MA", "NY", "CT"])

# Custom Check: Row Count
# Check that there are exactly 0 rows where age is negative
df.quality.expect_row_count("age < 0", 0, "eq")

# Check that there are at least 100 rows total
df.quality.expect_row_count("1=1", 100, "ge")
```

```
