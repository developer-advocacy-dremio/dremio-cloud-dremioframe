import pytest
from unittest.mock import MagicMock
from dremioframe.client import DremioClient
from dremioframe.admin import Admin
from dremioframe.catalog import Catalog

class TestUrlBuilding:
    def test_cloud_url_building(self):
        client = MagicMock(spec=DremioClient)
        client.mode = "cloud"
        client.base_url = "https://api.dremio.cloud/v0"
        client.project_id = "test-project-id"
        
        admin = Admin(client)
        url = admin._build_url("reflection")
        assert url == "https://api.dremio.cloud/v0/projects/test-project-id/reflection"
        
        catalog = Catalog(client)
        url = catalog._build_url("catalog")
        assert url == "https://api.dremio.cloud/v0/projects/test-project-id/catalog"

    def test_software_url_building(self):
        client = MagicMock(spec=DremioClient)
        client.mode = "v26"
        client.base_url = "https://dremio.example.com:9047/api/v3"
        client.project_id = None
        
        admin = Admin(client)
        url = admin._build_url("reflection")
        assert url == "https://dremio.example.com:9047/api/v3/reflection"
        
        catalog = Catalog(client)
        url = catalog._build_url("catalog")
        assert url == "https://dremio.example.com:9047/api/v3/catalog"
