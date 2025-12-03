# Query Cost Estimation

DremioFrame provides a `CostEstimator` to analyze query execution plans, estimate costs, and suggest optimizations before running expensive queries.

## CostEstimator

The `CostEstimator` uses Dremio's `EXPLAIN PLAN` to analyze queries and provide actionable insights.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.cost_estimator import CostEstimator

client = DremioClient()
estimator = CostEstimator(client)
```

### Estimating Query Cost

Get a detailed cost estimate for any query:

```python
sql = """
SELECT customer_id, SUM(amount) as total
FROM sales.transactions
WHERE date >= '2024-01-01'
GROUP BY customer_id
"""

estimate = estimator.estimate_query_cost(sql)

print(f"Estimated rows: {estimate.estimated_rows}")
print(f"Total cost: {estimate.total_cost}")
print(f"Plan summary: {estimate.plan_summary}")
print(f"Optimization hints: {estimate.optimization_hints}")
```

**Output**:
```
Estimated rows: 50000
Total cost: 125.5
Plan summary: Query plan includes: 1 table scan(s), aggregation, 1 filter(s)
Optimization hints: ['Consider adding LIMIT for large tables']
```

### Cost Estimate Details

The `CostEstimate` object includes:
- **estimated_rows**: Number of rows expected to be processed
- **estimated_bytes**: Approximate data size
- **scan_cost**: Cost of table scans
- **join_cost**: Cost of join operations
- **total_cost**: Overall query cost metric
- **plan_summary**: Human-readable plan description
- **optimization_hints**: List of suggestions

### Optimization Hints

The estimator automatically detects common anti-patterns:

```python
hints = estimator.get_optimization_hints(sql)

for hint in hints:
    print(f"{hint.severity}: {hint.message}")
    print(f"  Suggestion: {hint.suggestion}")
```

**Common hints**:
- **SELECT \***: Suggests specifying only needed columns
- **Missing WHERE**: Warns about full table scans
- **Multiple JOINs**: Suggests using CTEs for readability
- **ORDER BY without LIMIT**: Recommends adding LIMIT
- **DISTINCT usage**: Suggests alternatives like GROUP BY

### Comparing Query Variations

Compare multiple approaches to find the most efficient:

```python
result = estimator.compare_queries(
    # Approach 1: Subquery
    """
    SELECT * FROM (
        SELECT customer_id, amount FROM sales.transactions
    ) WHERE amount > 1000
    """,
    
    # Approach 2: Direct filter
    """
    SELECT customer_id, amount 
    FROM sales.transactions 
    WHERE amount > 1000
    """
)

print(result['recommendation'])
# Output: "Query 2 has the lowest estimated cost (45.2)"

for query in result['queries']:
    print(f"Query {query['query_id']}: Cost = {query['total_cost']}")
```

## Use Cases

### 1. Pre-execution Validation

```python
# Check cost before running expensive query
estimate = estimator.estimate_query_cost(expensive_sql)

if estimate.total_cost > 1000:
    print("Warning: This query may be expensive!")
    print("Hints:", estimate.optimization_hints)
    # Decide whether to proceed
```

### 2. Query Optimization Workflow

```python
# Iteratively improve query
queries = [
    "SELECT * FROM large_table",
    "SELECT id, name FROM large_table",
    "SELECT id, name FROM large_table WHERE active = true"
]

comparison = estimator.compare_queries(*queries)
print(f"Best approach: Query {comparison['best_query_id']}")
```

### 3. Automated Query Review

```python
# Review all queries in a pipeline
for sql in pipeline_queries:
    estimate = estimator.estimate_query_cost(sql)
    if len(estimate.optimization_hints) > 0:
        print(f"Query needs review: {sql[:50]}...")
        for hint in estimate.optimization_hints:
            print(f"  - {hint}")
```

## Limitations

- **Cost Metrics**: Costs are relative estimates, not absolute resource usage
- **Plan Parsing**: Based on Dremio's EXPLAIN output format (may vary by version)
- **Optimization Hints**: Static analysis; may not catch all issues
- **Reflection Impact**: Doesn't account for reflection acceleration

## Best Practices

1. **Use Early**: Check costs during development, not just production
2. **Iterate**: Use `compare_queries()` to test different approaches
3. **Combine with Profiling**: Use cost estimation for planning, profiling for actual performance
4. **Set Thresholds**: Define acceptable cost limits for your use case
