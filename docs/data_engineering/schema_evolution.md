# Schema Evolution

DremioFrame provides tools to manage schema evolution for your Dremio tables, allowing you to detect changes and generate migration scripts.

## SchemaManager

The `SchemaManager` class helps you compare the current schema of a table in Dremio against a desired schema (e.g., from your code or a config file) and synchronize them.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.schema_evolution import SchemaManager

client = DremioClient()
manager = SchemaManager(client)
```

### Comparing Schemas

You can compare the current table schema with a new schema definition.

```python
# Define desired schema
new_schema = {
    "id": "INT",
    "name": "VARCHAR",
    "email": "VARCHAR",
    "created_at": "TIMESTAMP"
}

# Get current schema
current_schema = manager.get_table_schema("space.folder.users")

# Compare
diff = manager.compare_schemas(current_schema, new_schema)

print("Added:", diff['added_columns'])
print("Removed:", diff['removed_columns'])
print("Changed:", diff['changed_columns'])
```

### Generating Migration Scripts

Generate SQL statements to migrate the table.

```python
sqls = manager.generate_migration_sql("space.folder.users", diff)

for sql in sqls:
    print(sql)
# Output:
# ALTER TABLE space.folder.users ADD COLUMN email VARCHAR
```

### Syncing Table

Automatically apply changes (or dry run).

```python
# Dry run (default) - returns SQL statements
sqls = manager.sync_table("space.folder.users", new_schema, dry_run=True)

# Execute changes
manager.sync_table("space.folder.users", new_schema, dry_run=False)
```

## Limitations

- **Type Changes**: Changing column types is complex and may not be supported directly by Dremio for all table formats. The tool generates a warning comment for type changes.
- **Data Migration**: This tool handles schema changes (DDL), not data transformation.
- **Iceberg Support**: Works best with Iceberg tables which support full schema evolution.
