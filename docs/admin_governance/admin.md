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

# Create a Service User (Software & Cloud)
client.admin.create_user(
    username="service_app_1",
    identity_type="SERVICE_USER"
)

# Manage Credentials (Service Users only)
# Create a client secret
secret = client.admin.credentials.create(
    user_id="user-uuid",
    label="app-secret-1"
)
print(f"Secret: {secret['clientSecret']}")

# List credentials
creds = client.admin.credentials.list("user-uuid")

# Delete credential
client.admin.credentials.delete("credential-id")

# Change password (Local Users)
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

### Source Management

Manage data sources (S3, Nessie, Postgres, etc.).

### List Sources

```python
sources = client.admin.list_sources()
```

### Get Source

```python
source = client.admin.get_source("my_source")
```

### Create Source

```python
# Generic
client.admin.create_source(
    name="my_postgres",
    type="POSTGRES",
    config={"hostname": "...", "username": "..."}
)

# S3 Helper
client.admin.create_source_s3(
    name="my_datalake",
    bucket_name="my-bucket",
    access_key="...",
    secret_key="..."
)
```

### Delete Source

```python
client.admin.delete_source("source_id")
```

## Reflection Management

You can manage Dremio reflections (Raw and Aggregation) using the `admin` interface.

```python
# List all reflections
reflections = client.admin.list_reflections()

# Create a Raw Reflection
client.admin.create_reflection(
    dataset_id="dataset-uuid",
    name="my_raw_reflection",
    type="RAW",
    display_fields=["col1", "col2"],
    distribution_fields=["col1"],
    partition_fields=["col2"],
    sort_fields=["col1"]
)

# Create an Aggregation Reflection
client.admin.create_reflection(
    dataset_id="dataset-uuid",
    name="my_agg_reflection",
    type="AGGREGATION",
    dimension_fields=["dim1", "dim2"],
    measure_fields=["measure1"],
    distribution_fields=["dim1"]
)

# Enable/Disable Reflection
client.admin.enable_reflection("reflection-id")
client.admin.disable_reflection("reflection-id")

# Delete Reflection
client.admin.delete_reflection("reflection-id")
```

## Row Access Policies

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
