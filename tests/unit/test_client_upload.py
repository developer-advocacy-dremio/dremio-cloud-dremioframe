import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Ensure we import from the local directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from dremioframe.client import DremioClient
import pyarrow as pa

@pytest.fixture
def mock_client():
    client = DremioClient(pat="test_pat", hostname="test.dremio.cloud")
    client.table = MagicMock()
    return client

@patch("pyarrow.csv.read_csv")
@patch("os.path.exists")
def test_upload_csv(mock_exists, mock_read_csv, mock_client):
    mock_exists.return_value = True
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_read_csv.return_value = mock_table
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.csv", "space.table")
    
    mock_read_csv.assert_called_once_with("test.csv")
    mock_client.table.assert_called_once_with("space.table")
    mock_builder.create.assert_called_once_with("space.table", data=mock_table)

@patch("pyarrow.json.read_json")
@patch("os.path.exists")
def test_upload_json(mock_exists, mock_read_json, mock_client):
    mock_exists.return_value = True
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_read_json.return_value = mock_table
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.json", "space.table")
    
    mock_read_json.assert_called_once_with("test.json")
    mock_builder.create.assert_called_once_with("space.table", data=mock_table)

@patch("pyarrow.parquet.read_table")
@patch("os.path.exists")
def test_upload_parquet(mock_exists, mock_read_parquet, mock_client):
    mock_exists.return_value = True
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_read_parquet.return_value = mock_table
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.parquet", "space.table")
    
    mock_read_parquet.assert_called_once_with("test.parquet")
    mock_builder.create.assert_called_once_with("space.table", data=mock_table)

@patch("os.path.exists")
def test_upload_file_not_found(mock_exists, mock_client):
    mock_exists.return_value = False
    with pytest.raises(FileNotFoundError):
        mock_client.upload_file("missing.csv", "space.table")

@patch("dremioframe.client.os.path.exists")
def test_upload_unknown_format(mock_exists, mock_client):
    mock_exists.return_value = True
    with pytest.raises(ValueError, match="Could not infer format"):
        mock_client.upload_file("test.txt", "space.table")

@patch("pandas.read_excel")
@patch("os.path.exists")
def test_upload_excel(mock_exists, mock_read_excel, mock_client):
    mock_exists.return_value = True
    import pandas as pd
    mock_df = pd.DataFrame({"a": [1, 2, 3]})
    mock_read_excel.return_value = mock_df
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.xlsx", "space.table")
    
    mock_read_excel.assert_called_once_with("test.xlsx")
    mock_builder.create.assert_called_once()

@patch("pandas.read_html")
@patch("os.path.exists")
def test_upload_html(mock_exists, mock_read_html, mock_client):
    mock_exists.return_value = True
    import pandas as pd
    mock_df = pd.DataFrame({"a": [1, 2, 3]})
    mock_read_html.return_value = [mock_df]
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.html", "space.table")
    
    mock_read_html.assert_called_once_with("test.html")
    mock_builder.create.assert_called_once()

@patch("fastavro.reader")
@patch("builtins.open", new_callable=MagicMock)
@patch("os.path.exists")
def test_upload_avro(mock_exists, mock_open, mock_fastavro_reader, mock_client):
    mock_exists.return_value = True
    mock_fastavro_reader.return_value = [{"a": 1}, {"a": 2}, {"a": 3}]
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.avro", "space.table")
    
    mock_open.assert_called_once_with("test.avro", "rb")
    mock_builder.create.assert_called_once()

@patch("pyarrow.orc.read_table")
@patch("os.path.exists")
def test_upload_orc(mock_exists, mock_read_orc, mock_client):
    mock_exists.return_value = True
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_read_orc.return_value = mock_table
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.orc", "space.table")
    
    mock_read_orc.assert_called_once_with("test.orc")
    mock_builder.create.assert_called_once()

@patch("lance.dataset")
@patch("os.path.exists")
def test_upload_lance(mock_exists, mock_lance_ds, mock_client):
    mock_exists.return_value = True
    mock_ds = MagicMock()
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_ds.to_table.return_value = mock_table
    mock_lance_ds.return_value = mock_ds
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.lance", "space.table")
    
    mock_lance_ds.assert_called_once_with("test.lance")
    mock_builder.create.assert_called_once()

@patch("pyarrow.feather.read_table")
@patch("os.path.exists")
def test_upload_feather(mock_exists, mock_read_feather, mock_client):
    mock_exists.return_value = True
    mock_table = pa.Table.from_pydict({"a": [1, 2, 3]})
    mock_read_feather.return_value = mock_table
    
    mock_builder = MagicMock()
    mock_client.table.return_value = mock_builder
    
    mock_client.upload_file("test.feather", "space.table")
    
    mock_read_feather.assert_called_once_with("test.feather")
    mock_builder.create.assert_called_once()
