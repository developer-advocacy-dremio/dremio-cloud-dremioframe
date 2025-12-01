import pytest
import sys
from unittest.mock import MagicMock
from dremioframe.client import DremioClient
from dremioframe.admin import Admin

@pytest.fixture
def mock_client():
    client = MagicMock(spec=DremioClient)
    client.base_url = "https://dremio.example.com/api/v3"
    client.session = MagicMock()
    client.execute = MagicMock()
    return client

@pytest.fixture
def admin(mock_client):
    return Admin(mock_client)

def test_create_folder(admin, mock_client):
    """Test create_folder uses SQL execution."""
    path = "my_space.my_folder"
    admin.create_folder(path)
    mock_client.execute.assert_called_once_with(f"CREATE FOLDER IF NOT EXISTS {path}")

def test_create_space(admin, mock_client):
    """Test create_space uses REST API."""
    space_name = "NewSpace"
    mock_response = MagicMock()
    mock_response.json.return_value = {"entityType": "space", "name": space_name}
    mock_client.session.post.return_value = mock_response

    result = admin.create_space(space_name)

    mock_client.session.post.assert_called_once_with(
        f"{mock_client.base_url}/catalog",
        json={"entityType": "space", "name": space_name}
    )
    assert result["name"] == space_name

def test_create_space_folder(admin, mock_client):
    """Test create_space_folder uses REST API."""
    space = "MySpace"
    folder = "MyFolder"
    mock_response = MagicMock()
    mock_response.json.return_value = {"entityType": "folder", "path": [space, folder]}
    mock_client.session.post.return_value = mock_response

    result = admin.create_space_folder(space, folder)

    mock_client.session.post.assert_called_once_with(
        f"{mock_client.base_url}/catalog",
        json={"entityType": "folder", "path": [space, folder]}
    )
    assert result["path"] == [space, folder]
