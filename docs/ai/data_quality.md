# AI Data Quality Tools

The `DremioAgent` can automatically generate Data Quality (DQ) recipes for your datasets.

## Features

### 1. Automated Recipe Generation
The agent inspects a dataset's schema (columns and types) and generates a YAML configuration file for the DremioFrame Data Quality framework. It intelligently suggests checks like `not_null` for IDs and `unique` for primary keys.

-   **Tool**: `generate_dq_checks(table)`
-   **Method**: `agent.generate_dq_recipe(table)`

## Usage Examples

### Generating a Recipe

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()

# Generate a DQ recipe for a table
table_name = "sales.transactions"
recipe = agent.generate_dq_recipe(table_name)

print("Generated DQ Recipe:")
print(recipe)

# You can save this to a file
with open("dq_checks.yaml", "w") as f:
    f.write(recipe)
```

### Example Output

```yaml
table: sales.transactions
checks:
  - column: transaction_id
    tests:
      - not_null
      - unique
  - column: amount
    tests:
      - positive
```
