# Complex Type Functions

Functions for working with complex types like Arrays, Maps, and Structs.

## Usage

```python
from dremioframe import F

# Flatten an array
df.select(F.flatten("items"))

# Convert from JSON
df.select(F.convert_from("json_col", "JSON"))

# Convert to JSON
df.select(F.convert_to("map_col", "JSON"))
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `flatten(col)` | Explodes a list into multiple rows. |
| `convert_from(col, type)` | Convert from a serialized format (e.g. 'JSON') to a complex type. |
| `convert_to(col, type)` | Convert a complex type to a serialized format (e.g. 'JSON'). |
