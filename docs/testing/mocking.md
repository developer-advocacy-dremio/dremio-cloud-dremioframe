# Mock/Testing Framework

DremioFrame provides a comprehensive testing framework to write tests without requiring a live Dremio connection.

## MockDremioClient

The `MockDremioClient` mimics the `DremioClient` interface, allowing you to configure query responses for testing.

### Basic Usage

```python
from dremioframe.testing import MockDremioClient
import pandas as pd

# Create mock client
client = MockDremioClient()

# Configure a response
users_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

client.add_response("SELECT * FROM users", users_df)

# Use in your code
result = client.sql("SELECT * FROM users").collect()
print(result)  # Returns the mocked DataFrame
```

### Query Matching

The mock client supports both exact and partial query matching:

```python
# Exact match
client.add_response("SELECT * FROM users", users_df)

# Partial match (matches any query containing "FROM users")
client.add_response("FROM users", users_df)

# This will match:
client.sql("SELECT id FROM users WHERE age > 25").collect()
```

### Query History

Track which queries were executed:

```python
client.sql("SELECT * FROM table1")
client.sql("SELECT * FROM table2")

# Check history
print(client.query_history)  # ['SELECT * FROM table1', 'SELECT * FROM table2']
print(client.get_last_query())  # 'SELECT * FROM table2'

# Clear history
client.clear_history()
```

## FixtureManager

Manage test data fixtures for consistent, reusable test datasets.

### Creating Fixtures

```python
from dremioframe.testing import FixtureManager

manager = FixtureManager()

# Create from data
test_data = [
    {'product_id': 1, 'name': 'Widget', 'price': 9.99},
    {'product_id': 2, 'name': 'Gadget', 'price': 19.99}
]

df = manager.create_fixture('products', test_data)
```

### Loading from Files

```python
# Load CSV
df = manager.load_csv('customers', 'tests/fixtures/customers.csv')

# Load JSON
df = manager.load_json('orders', 'tests/fixtures/orders.json')

# Retrieve loaded fixture
customers = manager.get('customers')
```

### Saving Fixtures

```python
# Save to CSV
manager.save_csv('products', 'tests/fixtures/products.csv')

# Save to JSON
manager.save_json('products', 'tests/fixtures/products.json')
```

## Test Assertions

Helper functions for common test assertions.

### DataFrame Equality

```python
from dremioframe.testing import assert_dataframes_equal

expected = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
actual = client.sql("SELECT * FROM test").collect()

assert_dataframes_equal(expected, actual)
```

### Schema Validation

```python
from dremioframe.testing import assert_schema_matches

result = client.sql("SELECT * FROM users").collect()

expected_schema = {
    'id': 'int64',
    'name': 'object',
    'age': 'int64'
}

assert_schema_matches(result, expected_schema)
```

### Query Validation

```python
from dremioframe.testing import assert_query_valid

sql = "SELECT * FROM users WHERE age > 25"
assert_query_valid(sql)  # Checks basic SQL syntax
```

### Row Count Assertions

```python
from dremioframe.testing import assert_row_count

result = client.sql("SELECT * FROM users").collect()

assert_row_count(result, 10, 'eq')   # Exactly 10 rows
assert_row_count(result, 5, 'gt')    # More than 5 rows
assert_row_count(result, 100, 'lt')  # Less than 100 rows
```

## Complete Test Example

```python
import pytest
from dremioframe.testing import (
    MockDremioClient,
    FixtureManager,
    assert_dataframes_equal,
    assert_schema_matches
)

@pytest.fixture
def mock_client():
    """Fixture providing a configured mock client"""
    client = MockDremioClient()
    
    # Set up test data
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    client.add_response("FROM users", users)
    return client

def test_user_query(mock_client):
    """Test querying users"""
    result = mock_client.sql("SELECT * FROM users WHERE age > 20").collect()
    
    # Assertions
    assert len(result) == 3
    assert_schema_matches(result, {
        'id': 'int64',
        'name': 'object',
        'age': 'int64'
    })
    
    # Verify query was executed
    assert "users" in mock_client.get_last_query()

def test_data_transformation(mock_client):
    """Test a data transformation pipeline"""
    # Your application code that uses the client
    raw_data = mock_client.sql("SELECT * FROM users").collect()
    
    # Transform
    transformed = raw_data[raw_data['age'] > 25]
    
    # Assert
    assert len(transformed) == 2
    assert all(transformed['age'] > 25)
```

## Integration with pytest

### Shared Fixtures

Create reusable fixtures in `conftest.py`:

```python
# tests/conftest.py
import pytest
from dremioframe.testing import MockDremioClient, FixtureManager

@pytest.fixture
def mock_client():
    return MockDremioClient()

@pytest.fixture
def fixture_manager():
    return FixtureManager(fixtures_dir='tests/fixtures')
```

### Parametrized Tests

```python
@pytest.mark.parametrize("age,expected_count", [
    (20, 3),
    (30, 2),
    (40, 0)
])
def test_age_filter(mock_client, age, expected_count):
    result = mock_client.sql(f"SELECT * FROM users WHERE age > {age}").collect()
    assert len(result) == expected_count
```

## Best Practices

1. **Use Fixtures**: Create pytest fixtures for common mock setups
2. **Realistic Data**: Use fixtures that mirror production data structure
3. **Test Isolation**: Clear query history between tests
4. **Partial Matching**: Use partial query matching for flexibility
5. **Schema Validation**: Always validate schema in addition to data

## Limitations

- **No Actual Execution**: Queries aren't validated against Dremio
- **Simple Matching**: Query matching is string-based, not semantic
- **No Side Effects**: Mock doesn't simulate Dremio-specific behaviors
- **In-Memory Only**: All data must fit in memory
