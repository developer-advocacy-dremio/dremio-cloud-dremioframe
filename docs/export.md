# Data Export

DremioFrame allows you to export query results to local files.

## CSV Export

```python
# Export to CSV
df.to_csv("output.csv", index=False)
```

## Parquet Export

```python
# Export to Parquet
df.to_parquet("output.parquet")
```

These methods internally collect the data to a Pandas DataFrame and call the respective Pandas export methods, so they support all standard Pandas arguments.
