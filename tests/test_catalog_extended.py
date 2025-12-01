import pytest
from unittest.mock import Mock, patch
from dremioframe.client import DremioClient
from dremioframe.catalog import Catalog

@pytest.fixture
def mock_client():
    client = Mock(spec=DremioClient)
    client.base_url = "http://localhost:9047/api/v3"
    client.project_id = "test-project"
    client.session = Mock()
    return client

@pytest.fixture
def catalog(mock_client):
    return Catalog(mock_client)

def test_get_wiki(catalog, mock_client):
    mock_client.session.get.return_value.status_code = 200
    mock_client.session.get.return_value.json.return_value = {"text": "Wiki content", "version": 1}
    
    wiki = catalog.get_wiki("test-id")
    
    assert wiki["text"] == "Wiki content"
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/projects/test-project/catalog/test-id/collaboration/wiki")

def test_get_wiki_404(catalog, mock_client):
    mock_client.session.get.return_value.status_code = 404
    
    wiki = catalog.get_wiki("test-id")
    
    assert wiki == {}

def test_get_tags(catalog, mock_client):
    mock_client.session.get.return_value.status_code = 200
    mock_client.session.get.return_value.json.return_value = {"tags": ["tag1", "tag2"], "version": 1}
    
    tags = catalog.get_tags("test-id")
    
    assert tags == ["tag1", "tag2"]
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/projects/test-project/catalog/test-id/collaboration/tag")

def test_set_tags(catalog, mock_client):
    mock_client.session.post.return_value.status_code = 200
    mock_client.session.post.return_value.json.return_value = {"tags": ["new-tag"], "version": 2}
    
    catalog.set_tags("test-id", ["new-tag"])
    
    mock_client.session.post.assert_called_with(
        "http://localhost:9047/api/v3/projects/test-project/catalog/test-id/collaboration/tag",
        json={"tags": ["new-tag"]}
    )

def test_create_view(catalog, mock_client):
    mock_client.session.post.return_value.status_code = 200
    mock_client.session.post.return_value.json.return_value = {"id": "new-view-id"}
    
    catalog.create_view(["Space", "View"], "SELECT 1")
    
    mock_client.session.post.assert_called_with(
        "http://localhost:9047/api/v3/projects/test-project/catalog",
        json={
            "entityType": "dataset",
            "type": "VIRTUAL",
            "path": ["Space", "View"],
            "sql": "SELECT 1"
        }
    )

def test_update_view_auto_tag(catalog, mock_client):
    # Mock get_entity_by_id to return current tag
    mock_client.session.get.return_value.status_code = 200
    mock_client.session.get.return_value.json.return_value = {"tag": "v1"}
    
    mock_client.session.put.return_value.status_code = 200
    mock_client.session.put.return_value.json.return_value = {"id": "view-id", "tag": "v2"}
    
    catalog.update_view("view-id", ["Space", "View"], "SELECT 2")
    
    # Verify fetch call
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/projects/test-project/catalog/view-id")
    
    # Verify update call
    mock_client.session.put.assert_called_with(
        "http://localhost:9047/api/v3/projects/test-project/catalog/view-id",
        json={
            "entityType": "dataset",
            "type": "VIRTUAL",
            "id": "view-id",
            "path": ["Space", "View"],
            "sql": "SELECT 2",
            "tag": "v1"
        }
    )

def test_create_view_with_builder(catalog, mock_client):
    # Mock a builder object
    mock_builder = Mock()
    mock_builder._compile_sql.return_value = "SELECT * FROM table"
    
    mock_client.session.post.return_value.status_code = 200
    mock_client.session.post.return_value.json.return_value = {"id": "new-view-id"}
    
    catalog.create_view(["Space", "View"], mock_builder)
    
    mock_client.session.post.assert_called_with(
        "http://localhost:9047/api/v3/projects/test-project/catalog",
        json={
            "entityType": "dataset",
            "type": "VIRTUAL",
            "path": ["Space", "View"],
            "sql": "SELECT * FROM table"
        }
    )
