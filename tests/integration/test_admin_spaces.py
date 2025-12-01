import pytest
import os
import uuid
from dremioframe.client import DremioClient

@pytest.fixture(scope="module")
def client():
    pat = os.getenv("DREMIO_PAT")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    
    # Use DREMIO_SOFTWARE_HOST if set, otherwise default (which might be Cloud)
    # But these tests are specifically for Software features (Spaces) or generic (Folders)
    # We need to know if we are testing against Software or Cloud to expect success/failure
    # for create_space.
    
    # For now, let's assume the environment is set up correctly for what we want to test.
    # Or we can check the URL.
    
    base_url = os.getenv("DREMIO_BASE_URL")
    hostname = os.getenv("DREMIO_HOST", "data.dremio.cloud")
    
    return DremioClient(pat=pat, base_url=base_url, hostname=hostname)

def test_create_folder_sql(client):
    """Test create_folder using SQL (Cloud/Iceberg)."""
    # This requires a valid context or full path.
    # In Cloud, "Spaces" are folders.
    # Let's try to create a top-level folder (Space in Cloud terms)
    folder_name = f"test_folder_{uuid.uuid4().hex[:8]}"
    
    try:
        client.admin.create_folder(folder_name)
        # Verify it exists? 
        # client.catalog.get_entity_by_path(folder_name)
    finally:
        # Cleanup
        try:
            client.execute(f"DROP FOLDER IF EXISTS {folder_name}")
        except Exception:
            pass

def test_create_space_software(client):
    """Test create_space (Software API)."""
    if "dremio.cloud" in client.base_url:
        pytest.skip("Skipping Software Space test on Cloud")
        
    space_name = f"test_space_{uuid.uuid4().hex[:8]}"
    
    try:
        client.admin.create_space(space_name)
        # Verify
        # client.admin.get_source(space_name) # Spaces are containers, might need catalog API
    finally:
        # Cleanup
        try:
            # Need a delete method for spaces/folders via API
            # For now, maybe just log it or try requests directly
            client.session.delete(f"{client.base_url}/catalog/by-path/{space_name}")
        except Exception:
            pass

def test_create_space_folder_software(client):
    """Test create_space_folder (Software API)."""
    if "dremio.cloud" in client.base_url:
        pytest.skip("Skipping Software Space Folder test on Cloud")

    space_name = f"test_space_f_{uuid.uuid4().hex[:8]}"
    folder_name = "subfolder"
    
    try:
        # Create Space first
        client.admin.create_space(space_name)
        
        # Create Folder in Space
        client.admin.create_space_folder(space_name, folder_name)
        
    finally:
        # Cleanup
        try:
             client.session.delete(f"{client.base_url}/catalog/by-path/{space_name}")
        except Exception:
            pass
