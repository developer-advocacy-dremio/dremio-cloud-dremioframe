import pytest
from unittest.mock import MagicMock, patch
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
import pandas as pd
import pyarrow as pa
import os

@pytest.fixture
def mock_client():
    client = DremioClient(pat="mock_pat")
    client.session = MagicMock()
    # Mock execute to return None
    client.execute = MagicMock()
    return client

@pytest.fixture
def mock_builder(mock_client):
    builder = DremioBuilder(mock_client, "test_table")
    # Mock collect to return a DataFrame
    df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    builder.collect = MagicMock(return_value=df)
    builder._execute_dml = MagicMock(return_value="OK")
    return builder

def test_charting(mock_builder):
    with patch("matplotlib.pyplot.savefig") as mock_save:
        # Test chart generation
        ax = mock_builder.chart(kind="line", x="x", y="y", save_to="chart.png")
        assert ax is not None
        mock_save.assert_called_with("chart.png")

def test_export(mock_builder):
    df = mock_builder.collect()
    
    with patch.object(pd.DataFrame, "to_csv") as mock_to_csv:
        mock_builder.to_csv("test.csv", index=False)
        mock_to_csv.assert_called_with("test.csv", index=False)
        
    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
        mock_builder.to_parquet("test.parquet")
        mock_to_parquet.assert_called_with("test.parquet")

def test_ingest_api_replace(mock_client):
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    mock_client.session.get.return_value = mock_response
    
    # Mock table().create()
    mock_builder = MagicMock()
    mock_client.table = MagicMock(return_value=mock_builder)
    
    mock_client.ingest_api("http://api.com", "target_table", mode="replace")
    
    # Verify fetch
    mock_client.session.get.assert_called_with("http://api.com", headers=None)
    
    # Verify delete/drop
    mock_client.execute.assert_called_with("DROP TABLE IF EXISTS target_table")
    
    # Verify create called with data
    mock_builder.create.assert_called()
    args, kwargs = mock_builder.create.call_args
    assert args[0] == "target_table"
    assert isinstance(kwargs['data'], pd.DataFrame)

def test_ingest_api_append(mock_client):
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 3, "name": "C"}]
    mock_client.session.get.return_value = mock_response
    
    # Mock max PK aggregation
    mock_builder = MagicMock()
    mock_client.table = MagicMock(return_value=mock_builder)
    
    # Mock collect for max PK
    mock_builder.agg.return_value.collect.return_value = pd.DataFrame({"m": [2]})
    
    mock_client.ingest_api("http://api.com", "target_table", mode="append", pk="id")
    
    # Verify insert called
    mock_builder.insert.assert_called()
    args, kwargs = mock_builder.insert.call_args
    assert args[0] == "target_table"
    # Data should be filtered (id > 2)
    df_arg = kwargs['data']
    assert len(df_arg) == 1
    assert df_arg.iloc[0]['id'] == 3

def test_ingest_api_merge(mock_client):
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "name": "Updated"}]
    mock_client.session.get.return_value = mock_response
    
    mock_builder = MagicMock()
    mock_client.table = MagicMock(return_value=mock_builder)
    
    mock_client.ingest_api("http://api.com", "target_table", mode="merge", pk="id")
    
    # Verify staging table creation
    # We can't easily check the random name, but we can check calls
    # 1. create staging
    # 2. merge staging to target
    # 3. drop staging
    
    assert mock_builder.create.called
    assert mock_builder.merge.called
    assert mock_client.execute.called # DROP TABLE

def test_list_files(mock_client):
    # Mock DremioBuilder
    with patch("dremioframe.client.DremioBuilder") as MockBuilder:
        mock_client.list_files("@source/folder")
        MockBuilder.assert_called_with(mock_client, "TABLE(LIST_FILES('@source/folder'))")
