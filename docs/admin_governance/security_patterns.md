# Security Patterns

This guide covers advanced security patterns using Dremio's governance features.

## 1. Row-Level Security (RLS) with Lookup Tables

Instead of hardcoding users in RLS policies, use a lookup table to manage permissions dynamically.

### Step 1: Create Lookup Table
Create a table `admin.permissions` mapping users to regions.

| user_email | region |
| :--- | :--- |
| alice@co.com | NY |
| bob@co.com | CA |

### Step 2: Create Policy Function
The function checks if the current user matches the region in the lookup table.

```sql
CREATE FUNCTION check_region_access(r VARCHAR) 
RETURNS BOOLEAN 
RETURN SELECT count(*) > 0 FROM admin.permissions WHERE user_email = query_user() AND region = r;
```

### Step 3: Apply Policy

```python
client.admin.apply_row_access_policy("sales", "check_region_access(region)")
```

## 2. Hierarchy-Based Access

Allow managers to see data for their entire hierarchy.

*   Store the hierarchy in a flattened table or use recursive CTEs (if supported) in the policy function.
*   Common pattern: `path` column (e.g., `/US/NY/Sales`). Policy: `user_path LIKE row_path || '%'`.

## 3. Column Masking Patterns

### Dynamic Masking based on Role

```python
# Mask email for non-HR users
client.admin.create_policy_function(
    "mask_email", 
    "email VARCHAR", 
    "VARCHAR", 
    "CASE WHEN is_member('HR') THEN email ELSE '***@***.com' END"
)

client.admin.apply_masking_policy("employees", "email", "mask_email(email)")
```

### Format Preserving Masking

If downstream tools expect a valid email format, mask the characters but keep the structure.

```sql
-- Simple example
CASE WHEN is_member('HR') THEN email ELSE 'user_' || hash(email) || '@masked.com' END
```
