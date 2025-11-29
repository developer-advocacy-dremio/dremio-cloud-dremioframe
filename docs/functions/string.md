# String Functions

Functions for string manipulation.

## Usage

```python
from dremioframe import F

df.select(
    F.upper(F.col("name")),
    F.concat(F.col("first"), F.lit(" "), F.col("last"))
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `upper(col)` | Convert to uppercase. |
| `lower(col)` | Convert to lowercase. |
| `concat(*cols)` | Concatenate strings. |
| `substr(col, start, length)` | Extract substring. |
| `trim(col)` | Trim whitespace from both ends. |
| `ltrim(col)` | Trim whitespace from left. |
| `rtrim(col)` | Trim whitespace from right. |
| `length(col)` | Length of string. |
| `replace(col, pattern, replacement)` | Replace substring. |
| `regexp_replace(col, pattern, replacement)` | Replace using regex. |
| `initcap(col)` | Capitalize first letter of each word. |
