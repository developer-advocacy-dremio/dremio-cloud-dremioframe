import pytest
from dremioframe.testing import MockDremioClient, FixtureManager
import pandas as pd

def test_mock_client_in_real_scenario():
    """Test using mock client in a realistic test scenario"""
    # Create mock client
    client = MockDremioClient()
    
    # Set up test data
    users_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    client.add_response("SELECT * FROM users", users_df)
    
    # Simulate application code
    result = client.sql("SELECT * FROM users WHERE age > 20").collect()
    
    # Verify
    assert len(result) == 3
    assert 'name' in result.columns
    
    # Check query was tracked
    assert len(client.query_history) == 1
    assert "users" in client.get_last_query()
    
    print(f"\\nMock client test passed. Query history: {client.query_history}")

def test_fixture_manager_workflow():
    """Test complete fixture manager workflow"""
    manager = FixtureManager()
    
    # Create test data
    test_data = [
        {'product_id': 1, 'name': 'Widget', 'price': 9.99},
        {'product_id': 2, 'name': 'Gadget', 'price': 19.99}
    ]
    
    # Create fixture
    df = manager.create_fixture('products', test_data)
    
    # Use in mock client
    client = MockDremioClient()
    client.add_response("SELECT * FROM products", df)
    
    result = client.sql("SELECT * FROM products").collect()
    
    assert len(result) == 2
    assert abs(result['price'].sum() - 29.98) < 0.01  # Float comparison with tolerance
    
    print(f"\\nFixture manager test passed. Loaded {len(result)} products")
