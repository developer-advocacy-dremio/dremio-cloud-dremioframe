# Aggregate Functions

Aggregate functions operate on a set of values to compute a single result.

## Usage

```python
from dremioframe import F

df.group_by("dept").agg(
    total=F.sum("salary"),
    count=F.count("*")
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `sum(col)` | Returns the sum of values in the column. |
| `avg(col)` | Returns the average of values in the column. |
| `min(col)` | Returns the minimum value. |
| `max(col)` | Returns the maximum value. |
| `count(col)` | Returns the count of non-null values. Use `*` for total rows. |
| `stddev(col)` | Returns the sample standard deviation. |
| `variance(col)` | Returns the sample variance. |
| `approx_distinct(col)` | Returns the approximate number of distinct values (HyperLogLog). |
