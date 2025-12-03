import pytest
import pandas as pd
import tempfile
import os
from dremioframe.testing import (
    MockDremioClient,
    FixtureManager,
    assert_dataframes_equal,
    assert_schema_matches,
    assert_query_valid,
    assert_row_count
)

# Test MockDremioClient

def test_mock_client_basic():
    """Test basic mock client functionality"""
    client = MockDremioClient()
    
    # Add a response
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
    client.add_response("SELECT * FROM test", df)
    
    # Execute query
    result = client.sql("SELECT * FROM test").collect()
    
    assert len(result) == 3
    assert list(result.columns) == ['id', 'name']

def test_mock_client_query_history():
    """Test query history tracking"""
    client = MockDremioClient()
    
    client.sql("SELECT 1")
    client.sql("SELECT 2")
    
    assert len(client.query_history) == 2
    assert client.get_last_query() == "SELECT 2"

def test_mock_client_partial_match():
    """Test partial query matching"""
    client = MockDremioClient()
    
    df = pd.DataFrame({'count': [100]})
    client.add_response("FROM test", df)
    
    # Should match any query containing "FROM test"
    result = client.sql("SELECT * FROM test WHERE id > 5").collect()
    
    assert len(result) == 1
    assert result['count'][0] == 100

# Test FixtureManager

def test_fixture_manager_create():
    """Test creating fixtures from data"""
    manager = FixtureManager()
    
    data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    
    df = manager.create_fixture('users', data)
    
    assert len(df) == 2
    assert 'users' in manager.list_fixtures()

def test_fixture_manager_get():
    """Test retrieving fixtures"""
    manager = FixtureManager()
    
    data = [{'id': 1}]
    manager.create_fixture('test', data)
    
    df = manager.get('test')
    assert len(df) == 1

def test_fixture_manager_save_load_csv():
    """Test saving and loading CSV fixtures"""
    manager = FixtureManager()
    
    data = [{'id': 1, 'value': 'test'}]
    manager.create_fixture('test', data)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager.fixtures_dir = tmpdir
        manager.save_csv('test')
        
        # Clear and reload
        manager.clear()
        assert len(manager.list_fixtures()) == 0
        
        manager.load_csv('test')
        df = manager.get('test')
        assert len(df) == 1
        assert df['value'][0] == 'test'

# Test Assertions

def test_assert_dataframes_equal_success():
    """Test successful DataFrame equality assertion"""
    df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    df2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    
    # Should not raise
    assert_dataframes_equal(df1, df2)

def test_assert_dataframes_equal_failure():
    """Test DataFrame equality assertion failure"""
    df1 = pd.DataFrame({'a': [1, 2]})
    df2 = pd.DataFrame({'a': [1, 3]})
    
    with pytest.raises(AssertionError):
        assert_dataframes_equal(df1, df2)

def test_assert_schema_matches_success():
    """Test successful schema matching"""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [1.0, 2.0, 3.0]
    })
    
    schema = {
        'id': 'int64',
        'name': 'object',
        'value': 'float64'
    }
    
    # Should not raise
    assert_schema_matches(df, schema)

def test_assert_schema_matches_failure():
    """Test schema matching failure"""
    df = pd.DataFrame({'id': [1, 2]})
    
    schema = {'id': 'int64', 'name': 'object'}
    
    with pytest.raises(AssertionError, match="Missing columns"):
        assert_schema_matches(df, schema)

def test_assert_query_valid_success():
    """Test valid query assertion"""
    # Should not raise
    assert_query_valid("SELECT * FROM table")
    assert_query_valid("INSERT INTO table VALUES (1, 2)")
    assert_query_valid("CREATE TABLE test (id INT)")

def test_assert_query_valid_failure():
    """Test invalid query assertion"""
    with pytest.raises(AssertionError):
        assert_query_valid("")
    
    with pytest.raises(AssertionError):
        assert_query_valid("INVALID QUERY")

def test_assert_row_count():
    """Test row count assertions"""
    df = pd.DataFrame({'a': [1, 2, 3]})
    
    # Should not raise
    assert_row_count(df, 3, 'eq')
    assert_row_count(df, 2, 'gt')
    assert_row_count(df, 4, 'lt')
    
    # Should raise
    with pytest.raises(AssertionError):
        assert_row_count(df, 5, 'eq')
