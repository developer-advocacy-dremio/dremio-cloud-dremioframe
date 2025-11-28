import requests
from typing import List, Dict, Any, Optional

class Catalog:
    def __init__(self, client):
        self.client = client

    def _get_project_id(self):
        if not self.client.project_id:
            raise ValueError("Project ID is not set.")
        return self.client.project_id

    def list_catalog(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog"
        
        if path:
             # If path is provided, we might need to use by-path endpoint or just list children of a folder
             # The API docs say: GET /v0/projects/{project_id}/catalog/by-path/{path}
             url = f"{self.client.base_url}/projects/{project_id}/catalog/by-path/{path}"

        response = self.client.session.get(url)
        response.raise_for_status()
        data = response.json()
        
        # If it's the root catalog, it returns 'data' list.
        # If it's a folder/source, it returns 'children' list.
        if "data" in data:
            return data["data"]
        elif "children" in data:
            return data["children"]
        else:
            return [data] # It might be a single entity if path pointed to a file/table

    def get_entity(self, path: str) -> Dict[str, Any]:
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog/by-path/{path}"
        response = self.client.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_entity_by_id(self, id: str) -> Dict[str, Any]:
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog/{id}"
        response = self.client.session.get(url)
        response.raise_for_status()
        return response.json()

    def create_source(self, name: str, source_type: str, config: Dict[str, Any]):
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog"
        payload = {
            "entityType": "source",
            "name": name,
            "type": source_type,
            "config": config
        }
        response = self.client.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def create_folder(self, path: List[str]):
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog"
        payload = {
            "entityType": "folder",
            "path": path
        }
        response = self.client.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def delete_catalog_item(self, id: str):
        project_id = self._get_project_id()
        url = f"{self.client.base_url}/projects/{project_id}/catalog/{id}"
        response = self.client.session.delete(url)
        response.raise_for_status()
