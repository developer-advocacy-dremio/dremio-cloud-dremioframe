# Data Quality Framework

DremioFrame includes a file-based Data Quality (DQ) framework to validate your data in Dremio.

## Requirements
```bash
pip install "dremioframe[dq]"
```

## Defining Tests

Tests are defined in YAML files. You can place them in any directory.

**Example: `tests/dq/sales_checks.yaml`**
```yaml
tests:
  - name: Validate Sales Table
    table: "Space.Folder.Sales"
    checks:
      - type: not_null
        column: order_id
        
      - type: unique
        column: order_id
        
      - type: values_in
        column: status
        values: ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]
        
      - type: row_count
        condition: "amount < 0"
        threshold: 0
        operator: eq  # Expect 0 rows where amount < 0
        
      - type: custom_sql
        condition: "discount > amount"
        error_msg: "Discount cannot be greater than amount"
```

## Running Tests

Use the CLI to run tests in a directory.

```bash
dremio-cli dq run tests/dq
```

## Check Types

| Type | Description | Parameters |
|------|-------------|------------|
| `not_null` | Ensures a column has no NULL values. | `column` |
| `unique` | Ensures a column has unique values. | `column` |
| `values_in` | Ensures column values are within a list. | `column`, `values` |
| `row_count` | Checks row count matching a condition. | `condition`, `threshold`, `operator` (eq, ne, gt, lt, ge, le) |
| `custom_sql` | Fails if any row matches the condition. | `condition`, `error_msg` |
