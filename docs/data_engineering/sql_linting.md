# SQL Linting

DremioFrame provides a `SqlLinter` to validate SQL queries against Dremio and perform static code analysis to catch common issues.

## SqlLinter

The `SqlLinter` can validate queries by requesting an execution plan from Dremio (ensuring syntax and table references are correct) and by checking for patterns that might lead to poor performance or unexpected results.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.linter import SqlLinter

client = DremioClient()
linter = SqlLinter(client)
```

### Validating SQL

Validation runs `EXPLAIN PLAN FOR <query>` against Dremio. This confirms that the SQL syntax is valid and that all referenced tables and columns exist and are accessible.

```python
sql = "SELECT count(*) FROM space.folder.table"
result = linter.validate_sql(sql)

if result["valid"]:
    print("SQL is valid!")
else:
    print(f"Validation failed: {result['error']}")
```

### Static Linting

Static linting checks the SQL string for common anti-patterns without connecting to Dremio.

```python
sql = "SELECT * FROM huge_table"
warnings = linter.lint_sql(sql)

for warning in warnings:
    print(f"Warning: {warning}")
# Output: Warning: Avoid 'SELECT *' in production queries. Specify columns explicitly.
```

### Rules Checked

- **SELECT \***: Discourages selecting all columns in production.
- **Unbounded DELETE/UPDATE**: Warns if `DELETE` or `UPDATE` statements are missing a `WHERE` clause.
