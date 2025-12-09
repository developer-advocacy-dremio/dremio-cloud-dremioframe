import pytest
import pandas as pd
import polars as pl
import pyarrow as pa
from dremioframe.client import DremioClient
import os
import time


@pytest.mark.cloud
class TestCreateTableIntegration:
    """Integration tests for create_table method against live Dremio"""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create a DremioClient for testing"""
        client = DremioClient()
        
        # Wake up engines with a simple query
        print("Waking up Dremio engines...")
        try:
            client.execute("SELECT 1 as wake_up")
            time.sleep(2)  # Give engines a moment to fully wake
            print("Engines ready!")
        except Exception as e:
            print(f"Warning: Could not wake engines: {e}")
        
        return client
    
    @pytest.fixture(scope="class")
    def test_space(self):
        """Get the test space name from environment"""
        return os.getenv("TEST_FOLDER", "testing")
    
    @pytest.fixture(autouse=True)
    def cleanup(self, client, test_space):
        """Cleanup test tables after each test"""
        yield
        # Cleanup after test
        test_tables = [
            f"{test_space}.test_create_dict",
            f"{test_space}.test_create_pandas",
            f"{test_space}.test_create_polars",
            f"{test_space}.test_create_arrow",
            f"{test_space}.test_create_with_data",
            f"{test_space}.test_create_empty_from_df",
        ]
        
        for table in test_tables:
            try:
                client.execute(f'DROP TABLE IF EXISTS "{test_space}"."' + table.split('.')[-1] + '"')
            except Exception:
                pass
    
    def test_create_table_with_schema_dict(self, client, test_space):
        """Test creating an empty table with schema dictionary"""
        table_name = f"{test_space}.test_create_dict"
        
        schema = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "email": "VARCHAR",
            "created_at": "TIMESTAMP",
            "is_active": "BOOLEAN"
        }
        
        # Create the table
        client.create_table(table_name, schema=schema)
        
        # Verify table exists and has correct schema
        result = client.query(f"SELECT * FROM {table_name} LIMIT 0", format="pandas")
        
        assert len(result.columns) == 5
        assert "id" in result.columns
        assert "name" in result.columns
        assert "email" in result.columns
        assert "created_at" in result.columns
        assert "is_active" in result.columns
    
    def test_create_table_from_pandas_empty(self, client, test_space):
        """Test creating an empty table from pandas DataFrame schema"""
        table_name = f"{test_space}.test_create_pandas"
        
        # Create a DataFrame with schema but no data
        df = pd.DataFrame({
            "id": pd.Series([], dtype='int64'),
            "name": pd.Series([], dtype='str'),
            "score": pd.Series([], dtype='float64'),
            "active": pd.Series([], dtype='bool')
        })
        
        # Create table without inserting data
        client.create_table(table_name, schema=df, insert_data=False)
        
        # Verify table exists
        result = client.query(f"SELECT * FROM {table_name} LIMIT 0", format="pandas")
        
        assert len(result.columns) == 4
        assert "id" in result.columns
        assert "name" in result.columns
        assert "score" in result.columns
        assert "active" in result.columns
    
    def test_create_table_from_polars_empty(self, client, test_space):
        """Test creating an empty table from polars DataFrame schema"""
        table_name = f"{test_space}.test_create_polars"
        
        # Create a polars DataFrame with schema
        df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "name": pl.Series([], dtype=pl.Utf8),
            "amount": pl.Series([], dtype=pl.Float64)
        })
        
        # Create table without inserting data
        client.create_table(table_name, schema=df, insert_data=False)
        
        # Verify table exists
        result = client.query(f"SELECT * FROM {table_name} LIMIT 0", format="pandas")
        
        assert len(result.columns) == 3
        assert "id" in result.columns
        assert "name" in result.columns
        assert "amount" in result.columns
    
    def test_create_table_from_arrow_empty(self, client, test_space):
        """Test creating an empty table from Arrow Table schema"""
        table_name = f"{test_space}.test_create_arrow"
        
        # Create an Arrow table with schema
        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.float64())
        ])
        arrow_table = pa.table({
            'id': pa.array([], type=pa.int64()),
            'name': pa.array([], type=pa.string()),
            'value': pa.array([], type=pa.float64())
        }, schema=schema)
        
        # Create table without inserting data
        client.create_table(table_name, schema=arrow_table, insert_data=False)
        
        # Verify table exists
        result = client.query(f"SELECT * FROM {table_name} LIMIT 0", format="pandas")
        
        assert len(result.columns) == 3
        assert "id" in result.columns
        assert "name" in result.columns
        assert "value" in result.columns
    
    def test_create_table_with_data_from_dataframe(self, client, test_space):
        """Test creating a table and inserting data from DataFrame"""
        table_name = f"{test_space}.test_create_with_data"
        
        # Create a DataFrame with data
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "score": [95.5, 87.3, 92.1, 88.7, 91.2]
        })
        
        # Create table and insert data
        client.create_table(table_name, schema=df, insert_data=True)
        
        # Verify table exists and has data
        result = client.query(f"SELECT * FROM {table_name} ORDER BY id", format="pandas")
        
        assert len(result) == 5
        assert result["id"].tolist() == [1, 2, 3, 4, 5]
        assert result["name"].tolist() == ["Alice", "Bob", "Charlie", "David", "Eve"]
        assert len(result.columns) == 3
    
    def test_create_table_with_schema_dict_and_data(self, client, test_space):
        """Test creating a table with schema dict and inserting data separately"""
        table_name = f"{test_space}.test_create_empty_from_df"
        
        # Define schema
        schema = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "value": "DOUBLE"
        }
        
        # Create data
        data = pd.DataFrame({
            "id": [10, 20, 30],
            "name": ["X", "Y", "Z"],
            "value": [1.1, 2.2, 3.3]
        })
        
        # Create table with schema and data
        client.create_table(table_name, schema=schema, data=data, insert_data=True)
        
        # Verify table has data
        result = client.query(f"SELECT * FROM {table_name} ORDER BY id", format="pandas")
        
        assert len(result) == 3
        assert result["id"].tolist() == [10, 20, 30]
        assert result["name"].tolist() == ["X", "Y", "Z"]
    
    def test_create_table_various_types(self, client, test_space):
        """Test creating a table with various data types"""
        table_name = f"{test_space}.test_create_dict"
        
        schema = {
            "int_col": "INTEGER",
            "big_col": "BIGINT",
            "float_col": "FLOAT",
            "double_col": "DOUBLE",
            "bool_col": "BOOLEAN",
            "varchar_col": "VARCHAR",
            "date_col": "DATE",
            "timestamp_col": "TIMESTAMP"
        }
        
        # Create the table
        client.create_table(table_name, schema=schema)
        
        # Verify table exists
        result = client.query(f"SELECT * FROM {table_name} LIMIT 0", format="pandas")
        
        assert len(result.columns) == 8
