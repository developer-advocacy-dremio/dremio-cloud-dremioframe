# Data Quality YAML Syntax

DremioFrame's Data Quality framework allows you to define tests in YAML files. This enables a declarative approach to data quality, where checks are version-controlled and separated from your application code.

## File Structure

A Data Quality YAML file can contain a list of test definitions. Each test targets a specific table and contains a list of checks to perform.

### Root Element

The root of the YAML file can be:
1. A **list** of test objects.
2. A **dictionary** with a `tests` key containing a list of test objects.

### Test Object

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | No | A descriptive name for the test suite. Defaults to "Unnamed Test". |
| `table` | string | **Yes** | The full path to the Dremio table or view being tested (e.g., `source.folder.table`). |
| `checks` | list | **Yes** | A list of check objects to execute against the table. |

### Check Object

Every check object requires a `type` field. Other fields depend on the check type.

#### `not_null`
Ensures a column contains no NULL values.

| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | string | Must be `not_null`. |
| `column` | string | The name of the column to check. |

#### `unique`
Ensures all values in a column are unique.

| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | string | Must be `unique`. |
| `column` | string | The name of the column to check. |

#### `values_in`
Ensures column values are within a specified allowed list.

| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | string | Must be `values_in`. |
| `column` | string | The name of the column to check. |
| `values` | list | A list of allowed values (strings, numbers, etc.). |

#### `row_count`
Validates the total number of rows in the table based on a condition.

| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | string | Must be `row_count`. |
| `condition` | string | Optional SQL WHERE clause to filter rows before counting. Default: `1=1` (all rows). |
| `threshold` | number | The value to compare the count against. |
| `operator` | string | Comparison operator: `eq` (=), `gt` (>), `lt` (<), `gte` (>=), `lte` (<=). Default: `gt`. |

#### `custom_sql`
Runs a custom SQL condition that must return TRUE for the check to pass.

| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | string | Must be `custom_sql`. |
| `condition` | string | A SQL boolean expression (e.g., `SUM(amount) > 0`). |
| `error_msg` | string | Optional error message to display if the check fails. |

## Example

```yaml
tests:
  - name: "Customer Table Checks"
    table: "marketing.customers"
    checks:
      - type: "not_null"
        column: "customer_id"
      
      - type: "unique"
        column: "email"
        
      - type: "values_in"
        column: "status"
        values: ["active", "inactive", "pending"]

  - name: "Sales Data Validation"
    table: "sales.transactions"
    checks:
      - type: "row_count"
        threshold: 1000
        operator: "gt"
        
      - type: "custom_sql"
        condition: "SUM(total_amount) > 0"
        error_msg: "Total sales amount must be positive"
```
