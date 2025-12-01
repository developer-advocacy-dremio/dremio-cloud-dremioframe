import pytest
import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

# Use 'target' space as requested, or env var
TEST_SPACE = os.environ.get("DREMIO_TEST_SPACE", "testing")

@pytest.fixture
def client():
    pat = os.environ.get("DREMIO_PAT")
    project_id = os.environ.get("DREMIO_PROJECT_ID")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    return DremioClient(pat=pat, project_id=project_id)

def test_tagging_lifecycle(client):
    view_name = f"{TEST_SPACE}.test_tagging_view"
    
    # Cleanup
    try:
        # Try to find by path with slash
        path_str = f"{TEST_SPACE}/test_tagging_view"
        entity = client.catalog.get_entity(path_str)
        client.catalog.delete_catalog_item(entity['id'])
    except:
        pass

    # 1. Create View
    try:
        response = client.catalog.create_view([TEST_SPACE, "test_tagging_view"], "SELECT 1")
        view_id = response['id']
    except Exception as e:
        if hasattr(e, 'response'):
            print(f"Error response: {e.response.text}")
        raise e
    
    # view = client.catalog.get_entity(view_name)
    # view_id = view['id']

    # 2. Get Tags (should be empty)
    tags = client.catalog.get_tags(view_id)
    assert tags == []

    # 3. Set Tags
    # First get version (might be None if no tags yet, or empty tags have a version?)
    tag_info = client.catalog.get_tag_info(view_id)
    version = tag_info.get("version")
    
    new_tags = ["pii", "sensitive"]
    client.catalog.set_tags(view_id, new_tags, version=version)

    # 4. Verify Tags
    tags = client.catalog.get_tags(view_id)
    assert set(tags) == set(new_tags)

    # 5. Update Tags (overwrite)
    # Get new version
    tag_info = client.catalog.get_tag_info(view_id)
    version = tag_info.get("version")
    
    updated_tags = ["public"]
    client.catalog.set_tags(view_id, updated_tags, version=version)

    # 6. Verify Update
    tags = client.catalog.get_tags(view_id)
    assert tags == ["public"]

    # Cleanup
    client.catalog.delete_catalog_item(view_id)
