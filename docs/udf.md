# UDF Manager

DremioFrame provides a pythonic interface to manage SQL User Defined Functions (UDFs).

## Usage

Access the UDF manager via `client.udf`.

### Create a UDF

```python
# CREATE FUNCTION my_space.add_ints (x INT, y INT) RETURNS INT RETURN x + y

# Option 1: Using a dictionary for arguments
client.udf.create(
    name="my_space.add_ints",
    args={"x": "INT", "y": "INT"},
    returns="INT",
    body="x + y",
    replace=True
)

# Option 2: Using a string for arguments (useful for complex types)
client.udf.create(
    name="my_space.complex_func",
    args="x INT, y STRUCT<a INT, b INT>",
    returns="INT",
    body="x + y.a",
    replace=True
)
```

### Drop a UDF

```python
client.udf.drop("my_space.add_ints", if_exists=True)
```

### List UDFs

```python
# List all functions matching 'add'
funcs = client.udf.list(pattern="add")
for f in funcs:
    print(f["ROUTINE_NAME"])
```
