import pytest
import os
import uuid
import pandas as pd
from dremioframe.client import DremioClient

# Skip if no credentials
@pytest.mark.skipif(not os.getenv("DREMIO_PAT"), reason="No Dremio credentials")
def test_bulk_loading_integration_live():
    # 1. Setup Client
    client = DremioClient(
        pat=os.getenv("DREMIO_PAT"),
        project_id=os.getenv("DREMIO_PROJECT_ID")
    )
    
    # Setup unique folder
    NAMESPACE = os.getenv("TEST_FOLDER", "testing")
    TEST_ID = uuid.uuid4().hex[:8]
    TEST_FOLDER = f"bulk_test_{TEST_ID}"
    FULL_FOLDER_PATH = [NAMESPACE, TEST_FOLDER]
    
    # Create folder
    try:
        client.catalog.create_folder(FULL_FOLDER_PATH)
    except Exception as e:
        pytest.fail(f"Failed to create test folder: {e}")

    # 2. Create test data (larger dataset to benefit from staging)
    data = pd.DataFrame({
        "id": range(1000),
        "name": [f"user_{i}" for i in range(1000)],
        "score": [i * 0.5 for i in range(1000)]
    })
    
    # 3. Test CREATE with staging method
    table_name = f'"{NAMESPACE}"."{TEST_FOLDER}"."bulk_test"'
    
    try:
        # Create using staging method
        client.table(table_name).create(table_name, data=data, method="staging")
        
        # Verify data
        df = client.table(table_name).collect("pandas")
        assert len(df) == 1000
        assert "user_500" in df['name'].values
        
        # 4. Test INSERT with staging method
        more_data = pd.DataFrame({
            "id": range(1000, 1500),
            "name": [f"user_{i}" for i in range(1000, 1500)],
            "score": [i * 0.5 for i in range(1000, 1500)]
        })
        
        client.table(table_name).insert(table_name, data=more_data, method="staging")
        
        # Verify
        df = client.table(table_name).collect("pandas")
        assert len(df) == 1500
        
    finally:
        # Cleanup: Delete the folder
        try:
            items = client.catalog.list_catalog(NAMESPACE)
            folder_id = None
            for item in items:
                if item['path'][-1] == TEST_FOLDER:
                    folder_id = item['id']
                    break
            
            if folder_id:
                client.catalog.delete_catalog_item(folder_id)
        except Exception as e:
            print(f"Cleanup failed: {e}")
