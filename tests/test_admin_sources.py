import pytest
from unittest.mock import MagicMock
from dremioframe.admin import Admin

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.base_url = "http://localhost:9047/api/v3"
    client.project_id = None
    return client

def test_list_sources(mock_client):
    admin = Admin(mock_client)
    mock_client.session.get.return_value.json.return_value = {"data": []}
    
    admin.list_sources()
    
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/source")

def test_get_source_by_id(mock_client):
    admin = Admin(mock_client)
    mock_client.session.get.return_value.json.return_value = {"id": "123"}
    
    res = admin.get_source("123")
    
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/source/123")
    assert res["id"] == "123"

def test_get_source_by_path(mock_client):
    admin = Admin(mock_client)
    # Mock failure for ID lookup
    mock_client.session.get.side_effect = Exception("Not found")
    # Mock catalog lookup
    mock_client.catalog.get_entity.return_value = {"path": ["my_source"]}
    
    res = admin.get_source("my_source")
    
    mock_client.catalog.get_entity.assert_called_with("my_source")
    assert res["path"] == ["my_source"]

def test_create_source(mock_client):
    admin = Admin(mock_client)
    mock_client.session.post.return_value.json.return_value = {"id": "new_source"}
    
    admin.create_source("new_source", "S3", {"bucket": "b"})
    
    mock_client.session.post.assert_called()
    args, kwargs = mock_client.session.post.call_args
    assert kwargs["json"]["name"] == "new_source"
    assert kwargs["json"]["type"] == "S3"

def test_delete_source(mock_client):
    admin = Admin(mock_client)
    
    admin.delete_source("123")
    
    mock_client.session.delete.assert_called_with("http://localhost:9047/api/v3/source/123")

def test_create_source_s3(mock_client):
    admin = Admin(mock_client)
    
    admin.create_source_s3("s3_source", "my-bucket", "key", "secret")
    
    mock_client.session.post.assert_called()
    args, kwargs = mock_client.session.post.call_args
    config = kwargs["json"]["config"]
    assert config["bucketName"] == "my-bucket"
    assert config["accessKey"] == "key"
    assert config["accessSecret"] == "secret"
    assert config["authenticationType"] == "ACCESS_KEY"
