# Pydantic Integration

DremioFrame integrates with Pydantic to allow for schema validation and table creation based on data models.

## Creating Tables from Models

You can generate a `CREATE TABLE` statement directly from a Pydantic model.

```python
from pydantic import BaseModel
from dremioframe.client import DremioClient

class User(BaseModel):
    id: int
    name: str
    active: bool

client = DremioClient(...)
builder = client.builder

# Create table 'users' with columns id (INTEGER), name (VARCHAR), active (BOOLEAN)
builder.create_from_model("users", User)
```

## Validating Data

You can validate existing data in Dremio against a Pydantic schema. This fetches a sample of data and checks if it conforms to the model.

```python
# Validate the first 1000 rows of 'users' table
builder.table("users").validate(User, sample_size=1000)
```

## Inserting Data with Validation

When inserting data using `create` or `insert`, you can pass a `schema` argument to validate the data before insertion.

```python
data = [{"id": 1, "name": "Alice", "active": True}]
builder.insert("users", data, schema=User)
```
