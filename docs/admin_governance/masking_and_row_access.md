# Row Access and Column Masking

`dremioframe` provides methods to manage Dremio's Row Access and Column Masking policies, allowing you to secure your data dynamically.

## User Defined Functions (UDFs)

Policies rely on User Defined Functions (UDFs) to define the logic for access control and masking.

### Creating a UDF

Use `admin.create_udf` (which delegates to `client.udf.create`) to create a UDF.

```python
from dremioframe.client import DremioClient

client = DremioClient(pat="...", project_id="...")

# Create a masking UDF
client.admin.create_udf(
    name="target.protect_ssn",
    args="ssn VARCHAR",
    return_type="VARCHAR",
    body="CASE WHEN is_member('hr') THEN ssn ELSE '***-**-****' END",
    replace=True
)

# Create a row access UDF
client.admin.create_udf(
    name="target.region_filter",
    args="region VARCHAR",
    return_type="BOOLEAN",
    body="is_member('sales') OR region = 'public'",
    replace=True
)
```

### Dropping a UDF

```python
client.admin.drop_udf("target.protect_ssn", if_exists=True)
```

## Column Masking Policies

Column masking policies dynamically mask data in a column based on a UDF.

### Applying a Masking Policy

```python
# Apply the 'protect_ssn' UDF to the 'ssn' column of 'employees' table
client.admin.apply_masking_policy(
    table="target.employees",
    column="ssn",
    policy="target.protect_ssn(ssn)"
)
```

### Removing a Masking Policy

```python
# Unset the masking policy
client.admin.drop_masking_policy(
    table="target.employees",
    column="ssn",
    policy="target.protect_ssn" # Optional, but good practice to specify
)
```

## Row Access Policies

Row access policies filter rows based on a UDF.

### Applying a Row Access Policy

```python
# Apply the 'region_filter' UDF to the 'employees' table
client.admin.apply_row_access_policy(
    table="target.employees",
    policy="target.region_filter(region)"
)
```

### Removing a Row Access Policy

```python
# Drop the row access policy
client.admin.drop_row_access_policy(
    table="target.employees",
    policy="target.region_filter(region)"
)
```
