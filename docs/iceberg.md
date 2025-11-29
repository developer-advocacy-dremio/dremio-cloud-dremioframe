# Iceberg Client

Interact directly with Dremio's Iceberg Catalog using `pyiceberg`.

# Iceberg Client

Interact directly with Dremio's Iceberg Catalog using `pyiceberg`.

## Configuration

The Iceberg client requires specific configuration depending on whether you are using Dremio Cloud or Dremio Software.

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DREMIO_PAT` | Personal Access Token | Yes | None |
| `DREMIO_PROJECT_ID` | Project Name (Cloud) or Warehouse Name | Yes (Cloud) | None |
| `DREMIO_ICEBERG_URI` | Iceberg Catalog REST URI | No (Cloud), Yes (Software) | `https://catalog.dremio.cloud/api/iceberg` |

### Dremio Cloud

For Dremio Cloud, you typically only need `DREMIO_PAT` and `DREMIO_PROJECT_ID`.

```bash
export DREMIO_PAT="your_pat"
export DREMIO_PROJECT_ID="your_project_id"
```

### Dremio Software

For Dremio Software, you must specify the `DREMIO_ICEBERG_URI`.

```bash
export DREMIO_PAT="your_pat"
export DREMIO_ICEBERG_URI="http://dremio-host:9047/api/iceberg"
# DREMIO_PROJECT_ID can be any string for Software, but is required by PyIceberg
export DREMIO_PROJECT_ID="my_warehouse" 
```

## Usage

### Access the Client

```python
iceberg = client.iceberg
```

### List Namespaces

```python
namespaces = iceberg.list_namespaces()
print(namespaces)
```

### List Tables

```python
tables = iceberg.list_tables("my_namespace")
print(tables)
```

### Load Table

```python
table = iceberg.load_table("my_namespace.my_table")
print(table.schema())
```

### Append Data

Append a Pandas DataFrame to an Iceberg table.

```python
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

iceberg.append("my_namespace.my_table", df)
```

### Create Table

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType

schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
)

table = iceberg.create_table("my_namespace.new_table", schema)
```
