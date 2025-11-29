import pytest
from unittest.mock import MagicMock
from dremioframe.client import DremioClient

@pytest.fixture
def mock_client():
    client = DremioClient(pat="mock_pat")
    client.session = MagicMock()
    client.base_url = "https://api.dremio.cloud/v0"
    return client

def test_list_reflections(mock_client):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": []}
    mock_client.session.get.return_value = mock_response
    
    reflections = mock_client.admin.list_reflections()
    
    mock_client.session.get.assert_called_with("https://api.dremio.cloud/v0/reflection")
    assert reflections == {"data": []}

def test_create_reflection(mock_client):
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": "123", "name": "my_ref"}
    mock_client.session.post.return_value = mock_response
    
    result = mock_client.admin.create_reflection(
        dataset_id="ds1",
        name="my_ref",
        type="RAW",
        display_fields=["col1", "col2"]
    )
    
    expected_payload = {
        "name": "my_ref",
        "type": "RAW",
        "datasetId": "ds1",
        "enabled": True,
        "displayFields": [{"name": "col1"}, {"name": "col2"}]
    }
    
    mock_client.session.post.assert_called_with(
        "https://api.dremio.cloud/v0/reflection",
        json=expected_payload
    )
    assert result == {"id": "123", "name": "my_ref"}

def test_delete_reflection(mock_client):
    mock_response = MagicMock()
    mock_client.session.delete.return_value = mock_response
    
    result = mock_client.admin.delete_reflection("123")
    
    mock_client.session.delete.assert_called_with("https://api.dremio.cloud/v0/reflection/123")
    assert result is True

def test_enable_reflection(mock_client):
    # Mock GET response
    get_response = MagicMock()
    get_response.json.return_value = {"id": "123", "enabled": False, "tag": "v1"}
    mock_client.session.get.return_value = get_response
    
    # Mock PUT response
    put_response = MagicMock()
    put_response.json.return_value = {"id": "123", "enabled": True, "tag": "v2"}
    mock_client.session.put.return_value = put_response
    
    result = mock_client.admin.enable_reflection("123")
    
    mock_client.session.put.assert_called_with(
        "https://api.dremio.cloud/v0/reflection/123",
        json={"id": "123", "enabled": True, "tag": "v1"}
    )
    assert result["enabled"] is True
