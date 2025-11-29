# Administration

DremioFrame provides tools for managing your Dremio environment, including sources, folders, users, grants, and security policies.

## Catalog Management

Access via `client.catalog`.

### Sources

```python
# Create a new S3 source
config = {
    "accessKey": "...",
    "accessSecret": "...",
    "bucketName": "my-bucket"
}
client.catalog.create_source("my_s3", "S3", config)

# Delete a source
source_id = client.catalog.get_entity_by_path("my_s3")['id']
client.catalog.delete_catalog_item(source_id)
```

### Folders

```python
# Create a folder
client.catalog.create_folder(["space_name", "folder_name"])
```

## User & Security Management

Access via `client.admin`.

### Users

```python
# Create a user
client.admin.create_user("new_user", "password123")

# Change password
client.admin.alter_user_password("new_user", "new_password456")

# Delete user
client.admin.drop_user("new_user")
```

### Grants (RBAC)

Manage permissions for users and roles.

```python
# Grant SELECT on a table to a role
client.admin.grant("SELECT", "TABLE marketing.sales", to_role="DATA_ANALYST")

# Grant all privileges on a space to a user
client.admin.grant("ALL PRIVILEGES", "SPACE marketing", to_user="john.doe")

# Revoke privileges
client.admin.revoke("SELECT", "TABLE marketing.sales", from_role="DATA_ANALYST")
```

### Masking Policies

Column masking allows you to obscure sensitive data based on user roles.

1. **Create a Policy Function** (UDF)

```python
# Create a function that masks SSN for non-admins
client.admin.create_policy_function(
    name="mask_ssn",
    args="ssn VARCHAR",
    return_type="VARCHAR",
    body="CASE WHEN is_member('admin') THEN ssn ELSE '***-**-****' END"
)
```

2. **Apply Policy**

```python
# Apply to the 'ssn' column of 'employees' table
client.admin.apply_masking_policy("employees", "ssn", "mask_ssn(ssn)")
```

3. **Drop Policy**

```python
client.admin.drop_masking_policy("employees", "ssn")
```

### Row Access Policies

Row access policies filter rows based on user roles.

1. **Create a Policy Function**

```python
# Create a function that returns TRUE if user can see the row
client.admin.create_policy_function(
    name="region_filter",
    args="region VARCHAR",
    return_type="BOOLEAN",
    body="is_member('admin') OR is_member(region)"
)
```

2. **Apply Policy**

```python
# Apply to 'sales' table
client.admin.apply_row_access_policy("sales", "region_filter(region)")
```

3. **Drop Policy**

```python
client.admin.drop_row_access_policy("sales")
```
