# UDF Manager

DremioFrame provides a pythonic interface to manage SQL User Defined Functions (UDFs).

## Usage

Access the UDF manager via `client.udf`.

### Create a UDF

```python
# CREATE FUNCTION my_space.add_ints (x INT, y INT) RETURNS INT RETURN x + y
client.udf.create(
    name="my_space.add_ints",
    args={"x": "INT", "y": "INT"},
    returns="INT",
    body="x + y",
    replace=True # Use CREATE OR REPLACE
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
