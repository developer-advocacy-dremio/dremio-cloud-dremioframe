# dlt Integration

DremioFrame integrates with [dlt (Data Load Tool)](https://dlthub.com/) to allow you to easily ingest data from hundreds of sources (APIs, Databases, SaaS applications) directly into Dremio.

## Installation

To use the `dlt` integration, you must install the optional dependencies:

```bash
pip install dremioframe[ingest]
```

## Usage

The integration is exposed via `client.ingest.dlt()`. It accepts any `dlt` source or resource and loads it into a Dremio table.

### Example: Loading from an API

```python
import dlt
from dremioframe.client import DremioClient

# 1. Initialize Client
client = DremioClient()

# 2. Define a dlt resource (e.g., fetching from an API)
@dlt.resource(name="pokemon")
def get_pokemon():
    import requests
    url = "https://pokeapi.co/api/v2/pokemon?limit=10"
    response = requests.get(url).json()
    yield from response["results"]

# 3. Ingest into Dremio
# This will create (or replace) the table "space.folder.pokemon"
client.ingest.dlt(
    source=get_pokemon(),
    table_name='"my_space"."my_folder"."pokemon"',
    write_disposition="replace"
)
```

### Parameters

- **source**: A `dlt` source or resource object.
- **table_name**: The target table name in Dremio (e.g., `"Space"."Folder"."Table"`).
- **write_disposition**:
    - `"replace"`: Drop table if exists and create new.
    - `"append"`: Append data to existing table.
- **batch_size**: Number of records to process per batch (default: 10,000).

## Supported Sources

Since DremioFrame accepts standard `dlt` sources, you can use any of the [verified sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/) from the dlt Hub, including:

- Salesforce
- HubSpot
- Google Sheets
- Notion
- Stripe
- And many more...
