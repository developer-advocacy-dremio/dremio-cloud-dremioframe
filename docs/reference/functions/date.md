# Date & Time Functions

Functions for date and time manipulation.

## Usage

```python
from dremioframe import F

df.select(
    F.year(F.col("date")),
    F.date_add(F.col("date"), 7)
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `current_date()` | Current date. |
| `current_timestamp()` | Current timestamp. |
| `date_add(col, days)` | Add days to date. |
| `date_sub(col, days)` | Subtract days from date. |
| `date_diff(col1, col2)` | Difference in days between dates. |
| `to_date(col, fmt)` | Convert string to date. |
| `to_timestamp(col, fmt)` | Convert string to timestamp. |
| `year(col)` | Extract year. |
| `month(col)` | Extract month. |
| `day(col)` | Extract day. |
| `hour(col)` | Extract hour. |
| `minute(col)` | Extract minute. |
| `second(col)` | Extract second. |
| `extract(field, source)` | Extract specific field (e.g., 'YEAR' from date). |
