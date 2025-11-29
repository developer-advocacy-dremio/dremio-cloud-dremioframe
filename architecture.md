# Architecture

DremioFrame is designed to abstract the complexities of Dremio's REST API and Arrow Flight SQL interface into a user-friendly Python library.

## Components

### 1. DremioClient (`client.py`)
The main entry point. It manages authentication and holds references to the `Catalog` and `Builder` factories.

### 2. Catalog (`catalog.py`)
Handles all metadata and administrative operations using the Dremio REST API.
- List catalogs, sources, folders, tables.
- Create/Update/Delete sources and views.

### 3. DremioBuilder (`builder.py`)
Provides a fluent interface for constructing queries.
- **DremioBuilder**: A fluent query builder that generates SQL.
    - `select()`, `filter()`, `mutate()`: Basic operations.
    - `group_by()`, `agg()`: Aggregation.
    - `order_by()`, `distinct()`: Sorting and deduplication.
    - `join()`: Joining tables.
    - `insert()`, `merge()`, `create()`: Data ingestion, upsert, and CTAS.
    - `at_snapshot()`, `at_timestamp()`: Iceberg Time Travel.
    - `optimize()`, `vacuum()`: Iceberg Maintenance.
    - `chart()`, `to_csv()`, `to_parquet()`: Visualization and Export.
    - `quality`: Access to `DataQuality` checks.
- **Functions (`functions.py`)**: A module (`F`) providing SQL functions and Window API.
    - `Expr`: Chainable SQL expressions.
    - `Window`: Window specification builder.
    - Standard functions: Aggregates, Math, String, Date, Conditional.
### 4. DataQuality (`quality.py`)
Provides methods to run validation queries against the data defined by the builder.
- `expect_not_null`, `expect_unique`, `expect_values_in`, `expect_row_count`.

### 5. Utils (`utils.py`)
Helper functions for configuration, logging, and common transformations.

## Data Flow

1.  **User** instantiates `DremioClient`.
2.  **User** calls `client.catalog.list_catalog()` -> **REST API** -> JSON response.
3.  **User** calls `client.table("source.table")` -> Returns `DremioBuilder`.
4.  **User** chains methods `builder.filter(...)` -> Updates internal state.
5.  **User** calls `builder.collect()` -> Generates SQL -> **Arrow Flight** -> Arrow Table -> Dataframe.
