# Math Functions

Mathematical functions for numeric operations.

## Usage

```python
from dremioframe import F

df.select(
    F.abs(F.col("diff")),
    F.round(F.col("price"), 2)
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `abs(col)` | Absolute value. |
| `ceil(col)` | Ceiling (round up). |
| `floor(col)` | Floor (round down). |
| `round(col, scale=0)` | Round to specified decimal places. |
| `sqrt(col)` | Square root. |
| `exp(col)` | Exponential (e^x). |
| `ln(col)` | Natural logarithm. |
| `log(base, col)` | Logarithm with specified base. |
| `pow(col, power)` | Power (x^y). |
