import pytest
from unittest.mock import MagicMock, patch
from dremioframe.client import DremioClient

@pytest.fixture
def mock_client():
    client = DremioClient(pat="test")
    return client

def test_query_pandas(mock_client):
    with patch("dremioframe.builder.DremioBuilder.collect") as mock_collect:
        mock_collect.return_value = "pandas_df"
        
        res = mock_client.query("SELECT 1", format="pandas")
        
        assert res == "pandas_df"
        mock_collect.assert_called_with("pandas")

def test_query_arrow(mock_client):
    with patch("dremioframe.builder.DremioBuilder.collect") as mock_collect:
        mock_collect.return_value = "arrow_table"
        
        res = mock_client.query("SELECT 1", format="arrow")
        
        assert res == "arrow_table"
        mock_collect.assert_called_with("arrow")

def test_query_polars(mock_client):
    with patch("dremioframe.builder.DremioBuilder.collect") as mock_collect:
        mock_collect.return_value = "polars_df"
        
        res = mock_client.query("SELECT 1", format="polars")
        
        assert res == "polars_df"
        mock_collect.assert_called_with("polars")
