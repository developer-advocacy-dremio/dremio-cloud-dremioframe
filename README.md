# DremioFrame

DremioFrame is a Python library that provides an Ibis-like dataframe builder interface for interacting with Dremio Cloud. It allows you to list data, perform CRUD operations, and administer Dremio resources using a familiar API.

## Documentation

- [Architecture](architecture.md)
- [Catalog & Admin](docs/catalog.md)
- [Dataframe Builder](docs/builder.md)

## Installation

```bash
pip install .
```

## Usage

```python
from dremioframe.client import DremioClient

client = DremioClient(pat="YOUR_PAT", project_id="YOUR_PROJECT_ID")

# List catalog
print(client.catalog.list_catalog())

# Query data
df = client.table("Samples.samples.dremio.com.zips.json").select("city", "state").filter("state = 'MA'").collect()
print(df)

# Calculated Columns
df.mutate(total_pop="pop * 2").show()

# Insert with Batching
df_large = pd.DataFrame(...)
client.table("target").insert("target", data=df_large, batch_size=1000)

# Merge (Upsert)
client.table("target").merge(
    target_table="target",
    on="id",
    matched_update={"val": "source.val"},
    not_matched_insert={"id": "source.id", "val": "source.val"},
    data=df_upsert
)

# Data Quality
df.quality.expect_not_null("city")
df.quality.expect_row_count("pop > 1000000", 5, "ge") # Expect at least 5 cities with pop > 1M
```
