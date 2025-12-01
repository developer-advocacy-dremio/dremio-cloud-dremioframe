# Window Functions

Window functions operate on a set of rows related to the current row.

## Usage

```python
from dremioframe import F

window = F.Window.partition_by("dept").order_by("salary")

df.select(
    F.rank().over(window).alias("rank")
)
```

## Window Specification

Use `F.Window` to create a specification:
- `partition_by(*cols)`
- `order_by(*cols)`
- `rows_between(start, end)`
- `range_between(start, end)`

## Available Functions

| Function | Description |
| :--- | :--- |
| `rank()` | Rank with gaps. |
| `dense_rank()` | Rank without gaps. |
| `row_number()` | Unique row number. |
| `lead(col, offset, default)` | Value from following row. |
| `lag(col, offset, default)` | Value from preceding row. |
| `first_value(col)` | First value in window frame. |
| `last_value(col)` | Last value in window frame. |
| `ntile(n)` | Distribute rows into n buckets. |
