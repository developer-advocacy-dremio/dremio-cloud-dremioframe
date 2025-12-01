# AI SQL Optimization Tools

The `DremioAgent` can act as your personal database administrator, analyzing query plans and suggesting optimizations.

## Features

### 1. Query Plan Analysis
The agent runs `EXPLAIN PLAN` on your SQL query and analyzes the execution plan to identify bottlenecks, such as full table scans or expensive joins.

-   **Tool**: `optimize_query(query)`
-   **Method**: `agent.optimize_sql(query)`

## Usage Examples

### Optimizing a Query

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()

# A potentially slow query
query = """
SELECT * 
FROM sales.transactions t
JOIN crm.customers c ON t.customer_id = c.id
WHERE t.amount > 1000
"""

# Ask the agent to optimize it
optimization = agent.optimize_sql(query)

print("Optimization Suggestions:")
print(optimization)
```

### Example Output

The agent might suggest:
-   Creating a Raw Reflection on `sales.transactions` covering `customer_id` and `amount`.
-   Filtering `sales.transactions` before joining.
-   Using a specific join type if applicable.
