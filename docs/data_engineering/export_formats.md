# Export Formats

DremioFrame supports exporting query results to various file formats, including Parquet, CSV, JSON, and Delta Lake.

## Supported Formats

### Parquet

Export to Parquet format (using Pandas).

```python
df.to_parquet("output.parquet", compression="snappy")
```

### CSV

Export to CSV format.

```python
df.to_csv("output.csv", index=False)
```

### JSON

Export to JSON format.

```python
df.to_json("output.json", orient="records")
```

### Delta Lake

Export to Delta Lake format. This requires the optional `deltalake` dependency.

**Installation:**
```bash
pip install dremioframe[delta]
```

**Usage:**
```python
# Create or overwrite a Delta table
df.to_delta("path/to/delta_table", mode="overwrite")

# Append to existing Delta table
df.to_delta("path/to/delta_table", mode="append")
```

## Considerations

- **Memory Usage**: All export methods currently collect the query result into memory (Pandas DataFrame) before writing. For very large datasets, consider using Dremio's `CTAS` (Create Table As Select) to write directly to data sources within Dremio, or use the `staging` method in `create/insert` for bulk loading.
- **Performance**: Parquet is generally the most performant format for both reading and writing.
