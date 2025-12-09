import pytest
import pandas as pd
import polars as pl
import pyarrow as pa
from unittest.mock import Mock, MagicMock, patch
from dremioframe.client import DremioClient


class TestCreateTable:
    """Unit tests for the create_table method"""
    
    @pytest.fixture
    def mock_client(self):
        """Create a mock DremioClient for testing"""
        client = Mock(spec=DremioClient)
        client.execute = Mock(return_value=pl.DataFrame())
        client.table = Mock()
        client._quote_table_name = DremioClient._quote_table_name.__get__(client)
        return client
    
    def test_create_table_with_schema_dict(self, mock_client):
        """Test creating a table with a schema dictionary"""
        schema = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "created_at": "TIMESTAMP"
        }
        
        # Call the actual method
        DremioClient.create_table(mock_client, "test_space.test_table", schema=schema)
        
        # Verify execute was called with correct SQL
        mock_client.execute.assert_called_once()
        call_args = mock_client.execute.call_args[0][0]
        
        assert 'CREATE TABLE "test_space"."test_table"' in call_args
        assert '"id" INTEGER' in call_args
        assert '"name" VARCHAR' in call_args
        assert '"created_at" TIMESTAMP' in call_args
    
    def test_create_table_with_schema_dict_and_data(self, mock_client):
        """Test creating a table with schema dict and inserting data"""
        schema = {"id": "INTEGER", "name": "VARCHAR"}
        data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        
        # Mock the table().insert() chain
        mock_builder = Mock()
        mock_client.table.return_value = mock_builder
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=schema, data=data, insert_data=True)
        
        # Verify table was created
        mock_client.execute.assert_called_once()
        
        # Verify insert was called
        mock_builder.insert.assert_called_once_with("test_space.test_table", data=data)
    
    def test_create_table_from_pandas_dataframe(self, mock_client):
        """Test creating a table from a pandas DataFrame"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "active": [True, False, True]
        })
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=df, insert_data=False)
        
        # Verify execute was called
        mock_client.execute.assert_called_once()
        call_args = mock_client.execute.call_args[0][0]
        
        assert 'CREATE TABLE "test_space"."test_table"' in call_args
        assert '"id"' in call_args
        assert '"name"' in call_args
        assert '"age"' in call_args
        assert '"active"' in call_args
    
    def test_create_table_from_polars_dataframe(self, mock_client):
        """Test creating a table from a polars DataFrame"""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 87.3, 92.1]
        })
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=df, insert_data=False)
        
        # Verify execute was called
        mock_client.execute.assert_called_once()
        call_args = mock_client.execute.call_args[0][0]
        
        assert 'CREATE TABLE "test_space"."test_table"' in call_args
    
    def test_create_table_from_arrow_table(self, mock_client):
        """Test creating a table from a pyarrow Table"""
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        }
        arrow_table = pa.table(data)
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=arrow_table, insert_data=False)
        
        # Verify execute was called
        mock_client.execute.assert_called_once()
        call_args = mock_client.execute.call_args[0][0]
        
        assert 'CREATE TABLE "test_space"."test_table"' in call_args
    
    def test_create_table_with_dataframe_and_insert(self, mock_client):
        """Test creating a table from DataFrame and inserting data"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        mock_builder = Mock()
        mock_client.table.return_value = mock_builder
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=df, insert_data=True)
        
        # Verify table was created
        mock_client.execute.assert_called_once()
        
        # Verify insert was called
        mock_builder.insert.assert_called_once()
    
    def test_create_table_empty_schema_dict_raises_error(self, mock_client):
        """Test that empty schema dict raises ValueError"""
        with pytest.raises(ValueError, match="Schema dictionary cannot be empty"):
            DremioClient.create_table(mock_client, "test_space.test_table", schema={})
    
    def test_create_table_no_schema_raises_error(self, mock_client):
        """Test that missing schema raises ValueError"""
        with pytest.raises(ValueError, match="Either schema dict or schema DataFrame/Table must be provided"):
            DremioClient.create_table(mock_client, "test_space.test_table", schema=None)
    
    def test_create_table_invalid_schema_type_raises_error(self, mock_client):
        """Test that invalid schema type raises ValueError"""
        with pytest.raises(ValueError, match="Schema must be a dict"):
            DremioClient.create_table(mock_client, "test_space.test_table", schema="invalid")
    
    def test_quote_table_name(self, mock_client):
        """Test table name quoting"""
        # Simple name
        result = DremioClient._quote_table_name(mock_client, "table")
        assert result == '"table"'
        
        # Qualified name
        result = DremioClient._quote_table_name(mock_client, "space.folder.table")
        assert result == '"space"."folder"."table"'
        
        # Already quoted
        result = DremioClient._quote_table_name(mock_client, '"space"."table"')
        assert result == '"space"."table"'
    
    def test_type_mapping_coverage(self, mock_client):
        """Test that various Arrow types are mapped correctly"""
        # Create a DataFrame with various types
        df = pd.DataFrame({
            "int_col": pd.Series([1, 2], dtype='int32'),
            "float_col": pd.Series([1.5, 2.5], dtype='float64'),
            "str_col": ["a", "b"],
            "bool_col": [True, False],
            "date_col": pd.to_datetime(["2024-01-01", "2024-01-02"])
        })
        
        DremioClient.create_table(mock_client, "test_space.test_table", 
                                 schema=df, insert_data=False)
        
        call_args = mock_client.execute.call_args[0][0]
        
        # Verify SQL types are present
        assert 'INTEGER' in call_args or 'BIGINT' in call_args
        assert 'DOUBLE' in call_args or 'FLOAT' in call_args
        assert 'VARCHAR' in call_args
        assert 'BOOLEAN' in call_args
        assert 'TIMESTAMP' in call_args or 'DATE' in call_args
