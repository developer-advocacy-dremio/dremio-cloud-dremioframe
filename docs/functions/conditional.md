# Conditional Functions

Functions for conditional logic.

## Usage

```python
from dremioframe import F

# Coalesce
df.select(F.coalesce(F.col("phone"), F.col("email"), F.lit("Unknown")))

# Case When
df.select(
    F.when("age < 18", "Minor")
     .when("age < 65", "Adult")
     .otherwise("Senior").alias("age_group")
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `coalesce(*cols)` | Returns the first non-null value. |
| `when(cond, val).otherwise(val)` | CASE WHEN statement builder. |
