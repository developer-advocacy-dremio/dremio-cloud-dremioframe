# Query Templates

DremioFrame provides a simple template system for managing parameterized SQL queries, helping you build a library of reusable query patterns.

## TemplateLibrary

The `TemplateLibrary` allows you to register and render SQL templates using standard Python string substitution syntax (`$variable`).

### Usage

```python
from dremioframe.templates import library, TemplateLibrary

# Use the global library or create your own
my_lib = TemplateLibrary()

# Register a template
my_lib.register(
    name="user_activity",
    sql="SELECT * FROM logs WHERE user_id = $uid AND date >= '$start_date'",
    description="Get activity logs for a specific user"
)

# Render the query
sql = my_lib.render("user_activity", uid=123, start_date="2023-01-01")
print(sql)
# Output: SELECT * FROM logs WHERE user_id = 123 AND date >= '2023-01-01'
```

### Built-in Templates

DremioFrame comes with a few common templates pre-registered in the global `library`:

- **row_count**: `SELECT COUNT(*) as count FROM $table`
- **sample**: `SELECT * FROM $table LIMIT $limit`
- **distinct_values**: `SELECT DISTINCT $column FROM $table ORDER BY $column`

```python
# Example using built-in template
sql = library.render("row_count", table="my_table")
```

## Best Practices

- **Security**: This is a simple string substitution mechanism. **It does not prevent SQL injection** if you pass untrusted user input directly. Always validate inputs or use it for internal query generation where inputs are controlled.
- **Organization**: Group related templates into separate libraries or modules for better organization in large projects.
