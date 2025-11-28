# Aggregation

DremioFrame supports standard SQL aggregation using `group_by` and `agg`.

## Group By

Use `group_by` to group rows by one or more columns.

```python
df.group_by("state")
df.group_by("state", "city")
```

## Aggregation

Use `agg` to define aggregation expressions. The keys become the new column names.

```python
# Calculate average population by state
df.group_by("state").agg(
    avg_pop="AVG(pop)",
    max_pop="MAX(pop)",
    count="COUNT(*)"
).show()
```

This generates SQL like:
```sql
SELECT AVG(pop) AS avg_pop, MAX(pop) AS max_pop, COUNT(*) AS count 
FROM table 
GROUP BY state
```
