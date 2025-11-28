# Sorting and Distinct

## Order By

Use `order_by` to sort the results.

```python
# Sort by population descending
df.order_by("pop", ascending=False).show()

# Sort by state ascending, then city descending
df.order_by("state").order_by("city", ascending=False).show()
```

## Distinct

Use `distinct` to remove duplicate rows.

```python
# Get unique states
df.select("state").distinct().show()
```
