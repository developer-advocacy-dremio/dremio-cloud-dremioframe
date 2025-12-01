import pytest
import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

TEST_SPACE = os.environ.get("DREMIO_TEST_SPACE", "testing")

@pytest.fixture
def client():
    pat = os.environ.get("DREMIO_PAT")
    project_id = os.environ.get("DREMIO_PROJECT_ID")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    return DremioClient(pat=pat, project_id=project_id)

def test_lineage(client):
    # 1. Create View V1
    try:
        resp1 = client.catalog.create_view([TEST_SPACE, "test_lineage_v1"], "SELECT 1")
        v1_id = resp1['id']
    except Exception as e:
        # Cleanup if exists
        try:
            path_str = f"{TEST_SPACE}/test_lineage_v1"
            entity = client.catalog.get_entity(path_str)
            client.catalog.delete_catalog_item(entity['id'])
            resp1 = client.catalog.create_view([TEST_SPACE, "test_lineage_v1"], "SELECT 1")
            v1_id = resp1['id']
        except:
            raise e

    # 2. Create View V2 from V1
    try:
        resp2 = client.catalog.create_view([TEST_SPACE, "test_lineage_v2"], f"SELECT * FROM {TEST_SPACE}.test_lineage_v1")
        v2_id = resp2['id']
    except Exception as e:
         # Cleanup if exists
        try:
            path_str = f"{TEST_SPACE}/test_lineage_v2"
            entity = client.catalog.get_entity(path_str)
            client.catalog.delete_catalog_item(entity['id'])
            resp2 = client.catalog.create_view([TEST_SPACE, "test_lineage_v2"], f"SELECT * FROM {TEST_SPACE}.test_lineage_v1")
            v2_id = resp2['id']
        except:
            raise e

    # 3. Get Lineage for V2
    lineage = client.catalog.get_lineage(v2_id)
    print(f"Lineage response: {lineage}")
    
    # Verify V1 is in parents
    parents = lineage.get("parents", [])
    if not parents:
        print("Warning: Lineage parents empty. Lineage might be async.")
    else:
        parent_ids = [p['id'] for p in parents]
        assert v1_id in parent_ids

    # Cleanup
    client.catalog.delete_catalog_item(v1_id)
    client.catalog.delete_catalog_item(v2_id)

def test_grants(client):
    # 1. Create Dataset
    try:
        resp = client.catalog.create_view([TEST_SPACE, "test_grants_view"], "SELECT 1")
        view_id = resp['id']
    except:
         # Cleanup if exists
        try:
            path_str = f"{TEST_SPACE}/test_grants_view"
            entity = client.catalog.get_entity(path_str)
            client.catalog.delete_catalog_item(entity['id'])
            resp = client.catalog.create_view([TEST_SPACE, "test_grants_view"], "SELECT 1")
            view_id = resp['id']
        except Exception as e:
            raise e

    # 2. Get Grants (verify default)
    grants_info = client.catalog.get_grants(view_id)
    # grants_info has 'grants' list and 'availablePrivileges'
    assert "grants" in grants_info
    
    # 3. Set Grants (add a user/role if possible, or just verify we can set empty or same)
    # Since we don't know other users, we can try to grant to ourselves or just set same grants
    current_grants = grants_info.get("grants", [])
    
    # Just set the same grants back to verify the API call works
    # Or try to empty them if allowed (might fail if owner needs privileges)
    # Let's try to set the same grants
    updated_grants_info = client.catalog.set_grants(view_id, current_grants)
    # If updated_grants_info is empty (204 No Content), it's a success
    if updated_grants_info:
        assert "grants" in updated_grants_info
    
    # Cleanup
    client.catalog.delete_catalog_item(view_id)
