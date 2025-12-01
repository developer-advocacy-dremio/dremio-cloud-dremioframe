# AI Reflection Tools

The `DremioAgent` can help you manage and optimize Dremio Reflections, which are critical for query acceleration.

## Features

### 1. Reflection Management
The agent can list existing reflections and create new ones.

-   **Tool**: `list_reflections()`
-   **Tool**: `create_reflection(dataset_id, name, type, fields)`

### 2. Smart Recommendations
The agent can analyze a SQL query and recommend the optimal reflection configuration (Raw vs. Aggregation) to accelerate it.

-   **Method**: `agent.recommend_reflections(query)`

## Usage Examples

### Listing Reflections

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()

# List all reflections
response = agent.generate_script("List all reflections in the system")
print(response)
```

### Getting Recommendations

```python
query = """
SELECT 
    region, 
    SUM(sales_amount) as total_sales 
FROM sales.transactions 
GROUP BY region
"""

recommendation = agent.recommend_reflections(query)
print("Recommended Reflection:")
print(recommendation)
```

### Creating a Reflection

You can ask the agent to create a reflection based on a recommendation.

```python
agent.agent.invoke({"messages": [("user", "Create an aggregation reflection on 'sales.transactions' for the query I just showed you.")]})
```
