import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock, patch
from dremioframe.cli import app

runner = CliRunner()

@pytest.fixture
def mock_client():
    client = MagicMock()
    # Mock sql().collect()
    mock_df = MagicMock()
    mock_df.to_markdown.return_value = "| col1 | col2 |\n|---|---|\n| 1 | 2 |"
    client.sql.return_value.collect.return_value = mock_df
    
    # Mock catalog
    client.catalog.list_catalog.return_value = [{"path": ["source"], "type": "CONTAINER", "id": "1"}]
    
    # Mock reflections
    client.admin.list_reflections.return_value = {"data": [{"name": "ref1", "type": "RAW", "enabled": True, "datasetId": "ds1"}]}
    
    return client

def test_query_command(mock_client):
    with patch("dremioframe.cli.get_client", return_value=mock_client):
        result = runner.invoke(app, ["query", "SELECT * FROM table"])
        assert result.exit_code == 0
        assert "| col1 | col2 |" in result.stdout

def test_catalog_command(mock_client):
    with patch("dremioframe.cli.get_client", return_value=mock_client):
        result = runner.invoke(app, ["catalog"])
        assert result.exit_code == 0
        assert "source" in result.stdout

def test_reflections_command(mock_client):
    with patch("dremioframe.cli.get_client", return_value=mock_client):
        result = runner.invoke(app, ["reflections"])
        assert result.exit_code == 0
        assert "ref1" in result.stdout
