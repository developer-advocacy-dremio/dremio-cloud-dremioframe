import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import pyarrow as pa
from dremioframe.ingest.files import ingest_files

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.table.return_value = MagicMock()
    return client

def test_ingest_files_parquet(mock_client):
    # Mock glob to return file list
    with patch("glob.glob") as mock_glob, \
         patch("pyarrow.parquet.read_table") as mock_read_parquet:
             
        mock_glob.return_value = ["/data/file1.parquet", "/data/file2.parquet"]
        
        # Mock parquet tables
        table1 = pa.table({"id": [1], "val": ["a"]})
        table2 = pa.table({"id": [2], "val": ["b"]})
        mock_read_parquet.side_effect = [table1, table2]
        
        # Mock builder methods
        builder = mock_client.table.return_value
        
        ingest_files(mock_client, "/data/*.parquet", "target_table")
        
        # Verify glob was called
        mock_glob.assert_called_with("/data/*.parquet", recursive=False)
        
        # Verify parquet reads
        assert mock_read_parquet.call_count == 2
        
        # Verify create was called with combined data
        builder.create.assert_called()
        args, kwargs = builder.create.call_args
        assert kwargs['method'] == 'staging'

def test_ingest_files_csv(mock_client):
    with patch("glob.glob") as mock_glob, \
         patch("pyarrow.csv.read_csv") as mock_read_csv:
             
        mock_glob.return_value = ["/data/file1.csv"]
        
        table = pa.table({"id": [1], "val": ["a"]})
        mock_read_csv.return_value = table
        
        builder = mock_client.table.return_value
        
        ingest_files(mock_client, "/data/*.csv", "target_table", file_format="csv")
        
        # Verify csv read
        mock_read_csv.assert_called_with("/data/file1.csv")
        
        # Verify create
        builder.create.assert_called()

def test_ingest_files_no_match(mock_client):
    with patch("glob.glob") as mock_glob:
        mock_glob.return_value = []
        
        with pytest.raises(ValueError, match="No files found"):
            ingest_files(mock_client, "/data/*.parquet", "target_table")

def test_ingest_files_append_mode(mock_client):
    with patch("glob.glob") as mock_glob, \
         patch("pyarrow.parquet.read_table") as mock_read_parquet:
             
        mock_glob.return_value = ["/data/file1.parquet"]
        
        table = pa.table({"id": [1], "val": ["a"]})
        mock_read_parquet.return_value = table
        
        builder = mock_client.table.return_value
        
        ingest_files(mock_client, "/data/*.parquet", "target_table", write_disposition="append")
        
        # Verify insert was called (not create)
        builder.insert.assert_called()
