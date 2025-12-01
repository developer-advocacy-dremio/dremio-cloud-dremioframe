# File Upload

DremioFrame allows you to upload local files directly to Dremio as Iceberg tables.

## Supported Formats

*   **CSV** (`.csv`)
*   **JSON** (`.json`)
*   **Parquet** (`.parquet`)
*   **Excel** (`.xlsx`, `.xls`, `.ods`) - Requires `pandas`, `openpyxl`
*   **HTML** (`.html`) - Requires `pandas`, `lxml`
*   **Avro** (`.avro`) - Requires `fastavro`
*   **ORC** (`.orc`) - Requires `pyarrow`
*   **Lance** (`.lance`) - Requires `pylance`
*   **Feather/Arrow** (`.feather`, `.arrow`) - Requires `pyarrow`

## Usage

Use the `client.upload_file()` method.

```python
from dremioframe.client import DremioClient

client = DremioClient()

# Upload a CSV file
client.upload_file("data/sales.csv", "space.folder.sales_table")

# Upload an Excel file
client.upload_file("data/financials.xlsx", "space.folder.financials")

# Upload an Avro file
client.upload_file("data/users.avro", "space.folder.users")
```

## Arguments

*   `file_path` (str): Path to the local file.
*   `table_name` (str): Destination table name in Dremio (e.g., "space.folder.table").
*   `file_format` (str, optional): The format of the file ('csv', 'json', 'parquet', 'excel', 'html', 'avro', 'orc', 'lance', 'feather'). If not provided, it is inferred from the file extension.
*   `**kwargs`: Additional arguments passed to the underlying file reader.
    *   **CSV**: `pyarrow.csv.read_csv`
    *   **JSON**: `pyarrow.json.read_json`
    *   **Parquet**: `pyarrow.parquet.read_table`
    *   **Excel**: `pandas.read_excel`
    *   **HTML**: `pandas.read_html`
    *   **Avro**: `fastavro.reader`
    *   **ORC**: `pyarrow.orc.read_table`
    *   **Lance**: `lance.dataset`
    *   **Feather**: `pyarrow.feather.read_table`

## Example with Options

```python
# Upload Excel sheet "Sheet2"
client.upload_file("data.xlsx", "space.folder.data", sheet_name="Sheet2")
```
