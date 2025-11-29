# SQL Functions

DremioFrame provides a comprehensive set of SQL functions via `dremioframe.functions` (aliased as `F`).

## Categories

- [Aggregate Functions](functions/aggregate.md)
- [Math Functions](functions/math.md)
- [String Functions](functions/string.md)
- [Date & Time Functions](functions/date.md)
- [Window Functions](functions/window.md)
- [Conditional Functions](functions/conditional.md)
- [AI Functions](functions/ai.md)

## Usage

```python
from dremioframe import F

df.select(
    F.col("name"),
    F.upper(F.col("city")),
    F.sum("salary").over(F.Window.partition_by("dept"))
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
