# SQL Functions

DremioFrame provides a comprehensive set of SQL functions and a Window API via `dremioframe.functions` (aliased as `F` in `dremioframe`).

## Usage

```python
from dremioframe import F
from dremioframe.functions import col

# Basic usage
df.select(
    F.col("name"),
    F.upper(F.col("city")).alias("CITY_UPPER"),
    (F.col("salary") * 1.1).alias("new_salary")
)

# Aggregation
df.group_by("dept").agg(
    total_sales=F.sum("sales"),
    avg_salary=F.avg("salary")
)
```

## Expressions (`Expr`)

The `Expr` class allows you to build complex SQL expressions using Python operators.

- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparison**: `==`, `!=`, `>`, `<`, `>=`, `<=`
- **Logical**: `&` (AND), `|` (OR), `~` (NOT)
- **Methods**:
    - `alias(name)`: Rename the expression.
    - `cast(type)`: Cast to a SQL type.
    - `isin(values)`: Check if value is in a list.
    - `is_null()`, `is_not_null()`: Check for NULLs.

## Window Functions

Use `F.Window` to define window specifications.

```python
from dremioframe import F

window_spec = F.Window.partition_by("dept").order_by("salary")

df.mutate(
    rank=F.rank().over(window_spec),
    running_total=F.sum("sales").over(window_spec)
)
```

### Frame Specifications
- `rows_between(start, end)`
- `range_between(start, end)`

Example:
```python
w = F.Window.order_by("date").rows_between("UNBOUNDED PRECEDING", "CURRENT ROW")
```

## Available Functions

### Aggregates
`sum`, `avg`, `min`, `max`, `count`, `stddev`, `variance`, `approx_distinct`

### Math
`abs`, `ceil`, `floor`, `round`, `sqrt`, `exp`, `ln`, `log`, `pow`

### String
`upper`, `lower`, `concat`, `substr`, `trim`, `ltrim`, `rtrim`, `length`, `replace`, `regexp_replace`, `initcap`

### Date/Time
`current_date`, `current_timestamp`, `date_add`, `date_sub`, `date_diff`, `to_date`, `to_timestamp`, `year`, `month`, `day`, `hour`, `minute`, `second`, `extract`

### Conditional
`coalesce`, `when().otherwise()`

```python
# CASE WHEN
expr = F.when("age < 18", "Minor") \
        .when("age < 65", "Adult") \
        .otherwise("Senior")
```

### Window
`rank`, `dense_rank`, `row_number`, `lead`, `lag`, `first_value`, `last_value`, `ntile`
