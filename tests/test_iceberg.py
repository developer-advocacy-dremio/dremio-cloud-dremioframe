import pytest
from unittest.mock import MagicMock, patch
from dremioframe.iceberg import DremioIcebergClient

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.project_id = "test_project"
    client.pat = "test_pat"
    return client

def test_iceberg_initialization(mock_client):
    with patch("dremioframe.iceberg.load_catalog") as mock_load:
        iceberg = DremioIcebergClient(mock_client)
        catalog = iceberg.catalog
        
        mock_load.assert_called_once()
        args, kwargs = mock_load.call_args
        assert args[0] == "dremio"
        assert kwargs["uri"] == "https://catalog.dremio.cloud/api/iceberg"
        assert kwargs["warehouse"] == "test_project"
        assert kwargs["token"] == "test_pat"

def test_list_namespaces(mock_client):
    with patch("dremioframe.iceberg.load_catalog") as mock_load:
        mock_catalog = MagicMock()
        mock_load.return_value = mock_catalog
        
        iceberg = DremioIcebergClient(mock_client)
        iceberg.list_namespaces()
        
        mock_catalog.list_namespaces.assert_called_once()

def test_list_tables(mock_client):
    with patch("dremioframe.iceberg.load_catalog") as mock_load:
        mock_catalog = MagicMock()
        mock_load.return_value = mock_catalog
        
        iceberg = DremioIcebergClient(mock_client)
        iceberg.list_tables("ns")
        
        mock_catalog.list_tables.assert_called_with("ns")

def test_load_table(mock_client):
    with patch("dremioframe.iceberg.load_catalog") as mock_load:
        mock_catalog = MagicMock()
        mock_load.return_value = mock_catalog
        
        iceberg = DremioIcebergClient(mock_client)
        iceberg.load_table("ns.table")
        
        mock_catalog.load_table.assert_called_with("ns.table")
