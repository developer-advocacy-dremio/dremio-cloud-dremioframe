import pytest
import os
import uuid
import dlt
from dremioframe.client import DremioClient

# Skip if no credentials
@pytest.mark.skipif(not os.getenv("DREMIO_PAT"), reason="No Dremio credentials")
def test_dlt_integration_live():
    # 1. Setup Client
    client = DremioClient(
        pat=os.getenv("DREMIO_PAT"),
        project_id=os.getenv("DREMIO_PROJECT_ID")
    )
    
    # Setup unique folder
    NAMESPACE = "testing"
    TEST_ID = uuid.uuid4().hex[:8]
    TEST_FOLDER = f"dlt_test_{TEST_ID}"
    FULL_FOLDER_PATH = [NAMESPACE, TEST_FOLDER]
    
    # Create folder
    try:
        client.catalog.create_folder(FULL_FOLDER_PATH)
    except Exception as e:
        pytest.fail(f"Failed to create test folder: {e}")

    # 2. Define a simple dlt resource
    data = [
        {"id": 1, "name": "Alice", "score": 90.5},
        {"id": 2, "name": "Bob", "score": 85.0},
        {"id": 3, "name": "Charlie", "score": 92.0}
    ]
    
    @dlt.resource(name="students")
    def student_data():
        yield from data
        
    # 3. Ingest
    # Use valid path: Space.Folder.Table
    # Quote components for safety
    table_name = f'"{NAMESPACE}"."{TEST_FOLDER}"."students"'
    
    try:
        # Run ingestion
        client.ingest.dlt(student_data(), table_name, write_disposition="replace")
        
        # 4. Verify
        # Query the table
        df = client.table(table_name).collect("pandas")
        
        assert len(df) == 3
        assert "Alice" in df['name'].values
        assert 90.5 in df['score'].values
        
    finally:
        # Cleanup: Delete the folder (and content)
        try:
            # Find folder ID to delete
            items = client.catalog.list_catalog(NAMESPACE)
            folder_id = None
            for item in items:
                # Path is list, check last element
                if item['path'][-1] == TEST_FOLDER:
                    folder_id = item['id']
                    break
            
            if folder_id:
                client.catalog.delete_catalog_item(folder_id)
        except Exception as e:
            print(f"Cleanup failed: {e}")
