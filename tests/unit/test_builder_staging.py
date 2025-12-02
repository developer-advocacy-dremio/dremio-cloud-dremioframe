import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import pyarrow as pa
from dremioframe.builder import DremioBuilder

@pytest.fixture
def mock_client():
    client = MagicMock()
    return client

@pytest.fixture
def builder(mock_client):
    return DremioBuilder(mock_client, "test_table")

def test_create_staging(builder, mock_client):
    data = pd.DataFrame({"id": [1], "val": ["a"]})
    
    # Mock tempfile, os, parquet, uuid
    with patch("tempfile.NamedTemporaryFile") as mock_temp, \
         patch("os.path.exists") as mock_exists, \
         patch("os.remove") as mock_remove, \
         patch("pyarrow.parquet.write_table") as mock_write:
             
        # Setup temp file mock
        mock_file = MagicMock()
        mock_file.name = "/tmp/test.parquet"
        mock_temp.return_value.__enter__.return_value = mock_file
        
        mock_exists.return_value = True
        
        # Call create with staging
        builder.create("test_table", data, method="staging")
        
        # Verify parquet write
        assert mock_write.called
        
        # Verify upload
        mock_client.upload_file.assert_called_with("/tmp/test.parquet", "test_table", file_format="parquet")
        
        # Verify cleanup
        mock_remove.assert_called_with("/tmp/test.parquet")

def test_insert_staging(builder, mock_client):
    data = pd.DataFrame({"id": [1], "val": ["a"]})
    
    with patch("tempfile.NamedTemporaryFile") as mock_temp, \
         patch("os.path.exists") as mock_exists, \
         patch("os.remove") as mock_remove, \
         patch("pyarrow.parquet.write_table") as mock_write, \
         patch("uuid.uuid4") as mock_uuid:
             
        mock_file = MagicMock()
        mock_file.name = "/tmp/test.parquet"
        mock_temp.return_value.__enter__.return_value = mock_file
        
        mock_exists.return_value = True
        mock_uuid.return_value.hex = "12345678"
        
        # Mock _execute_dml to avoid Flight client creation
        builder._execute_dml = MagicMock()
    
        # Call insert with staging
        builder.insert("test_table", data, method="staging")
        
        # Verify upload to temp table
        expected_temp_table = "test_table_staging_12345678"
        mock_client.upload_file.assert_called_with("/tmp/test.parquet", expected_temp_table, file_format="parquet")
        
        # Verify insert from temp
        # insert calls _execute_dml
        # We need to check the SQL passed to _execute_dml
        # But _execute_dml calls _execute_flight.
        # Let's mock _execute_dml on the builder instance itself? 
        # Or mock client.execute for the drop table.
        
        # Verify drop temp table
        mock_client.execute.assert_called_with(f"DROP TABLE IF EXISTS {expected_temp_table}")
