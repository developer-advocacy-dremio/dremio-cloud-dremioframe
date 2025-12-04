# Notebook Integration

DremioFrame provides rich integration with Jupyter Notebooks and other interactive environments (VS Code, Colab), making it an excellent tool for data exploration and analysis.

## Features

- **Rich DataFrame Display**: Automatically displays query results as formatted HTML tables.
- **Progress Bars**: Shows download progress for large datasets using `tqdm`.
- **Magic Commands**: IPython magic commands for quick SQL execution.

## Installation

Ensure you have the notebook dependencies installed:

```bash
pip install dremioframe[notebook]
```

## Rich Display

When you display a `DremioBuilder` object in a notebook, it automatically executes a preview query (LIMIT 20) and displays the results as a formatted HTML table, along with the generated SQL.

```python
from dremioframe.client import DremioClient

client = DremioClient()
df = client.table("finance.bronze.trips")

# Displaying the builder object shows a preview
df
```

## Progress Bars

When collecting large datasets, you can enable a progress bar to track the download status.

```python
# Download with progress bar
pdf = df.collect(library='pandas', progress_bar=True)
```

## Magic Commands

DremioFrame includes IPython magic commands to simplify your workflow.

### Loading Magics

First, load the extension:

```python
%load_ext dremioframe.notebook
```

### Connecting

Connect to Dremio using `%dremio_connect`. You can pass arguments or rely on environment variables.

```python
%dremio_connect pat=YOUR_PAT project_id=YOUR_PROJECT_ID
```

### Executing SQL

Use `%%dremio_sql` to execute SQL queries directly in a cell.

```sql
%%dremio_sql my_result
SELECT 
    trip_date,
    passenger_count
FROM finance.bronze.trips
WHERE passenger_count > 2
LIMIT 100
```

The result is automatically displayed and saved to the variable `my_result` (if specified).
