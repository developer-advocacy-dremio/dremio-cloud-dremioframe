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

# Aggregation
df.group_by("state").agg(avg_pop="AVG(pop)").show()

# Joins
df.join("other_table", on="left_tbl.id = right_tbl.id").show()

# Iceberg Time Travel
df.at_snapshot("123456789").show()

# External Queries
client.external_query("Postgres", "SELECT * FROM users").show()

# Insert Data (Batched)
import pandas as pd
data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
client.table("my_table").insert("my_table", data=data, batch_size=1000)

# Merge (Upsert)
client.table("target").merge(
    target_table="target",
    on="id",
    matched_update={"name": "source.name"},
    not_matched_insert={"id": "source.id", "val": "source.val"},
    data=data
)

# Data Quality
df.quality.expect_not_null("city")
df.quality.expect_row_count("pop > 1000000", 5, "ge") # Expect at least 5 cities with pop > 1M
```
