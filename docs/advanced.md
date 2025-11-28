# Advanced Features

## External Queries

Dremio allows you to push queries directly to the underlying source, bypassing Dremio's SQL parser. This is useful for using source-specific SQL dialects or features not yet supported by Dremio.

```python
# Run a native Postgres query
df = client.external_query("Postgres", "SELECT * FROM users WHERE active = true")

# You can then chain DremioFrame operations on top
df.filter("age > 21").select("name").show()
```

This generates SQL like:
```sql
SELECT name FROM (
    SELECT * FROM TABLE(Postgres.EXTERNAL_QUERY('SELECT * FROM users WHERE active = true'))
) AS sub
WHERE age > 21
```
