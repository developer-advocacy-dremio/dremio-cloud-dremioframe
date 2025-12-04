# Raw SQL Querying

While DremioFrame provides a powerful Builder API, sometimes you just want to run raw SQL.

## Usage

Use the `query()` method on the `DremioClient`.

```python
# Return as Pandas DataFrame (default)
df = client.query('SELECT * FROM finance.bronze.transactions LIMIT 10')

# Return as Arrow Table
arrow_table = client.query('SELECT * FROM finance.bronze.transactions LIMIT 10', format="arrow")

# Return as Polars DataFrame
polars_df = client.query('SELECT * FROM finance.bronze.transactions LIMIT 10', format="polars")
```

## DDL/DML

For operations that don't return rows (like `CREATE VIEW`, `DROP TABLE`), use `execute()`:

```python
client.execute("DROP TABLE IF EXISTS my_table")
```
