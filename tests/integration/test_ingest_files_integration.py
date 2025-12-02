import pytest
import os
import uuid
import pandas as pd
import tempfile
from dremioframe.client import DremioClient

# Skip if no credentials
@pytest.mark.skipif(not os.getenv("DREMIO_PAT"), reason="No Dremio credentials")
def test_files_integration_live():
    # 1. Setup Client
    client = DremioClient(
        pat=os.getenv("DREMIO_PAT"),
        project_id=os.getenv("DREMIO_PROJECT_ID")
    )
    
    # Setup unique folder
    NAMESPACE = os.getenv("TEST_FOLDER", "testing")
    TEST_ID = uuid.uuid4().hex[:8]
    TEST_FOLDER = f"files_test_{TEST_ID}"
    FULL_FOLDER_PATH = [NAMESPACE, TEST_FOLDER]
    
    # Create folder
    try:
        client.catalog.create_folder(FULL_FOLDER_PATH)
    except Exception as e:
        pytest.fail(f"Failed to create test folder: {e}")

    # 2. Create temporary test files
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create 3 parquet files
        for i in range(3):
            df = pd.DataFrame({
                "id": range(i*100, (i+1)*100),
                "batch": [i] * 100,
                "value": [f"val_{j}" for j in range(i*100, (i+1)*100)]
            })
            file_path = os.path.join(temp_dir, f"data_{i}.parquet")
            df.to_parquet(file_path)
        
        # 3. Ingest files using glob pattern
        table_name = f'"{NAMESPACE}"."{TEST_FOLDER}"."files_test"'
        pattern = os.path.join(temp_dir, "data_*.parquet")
        
        client.ingest.files(
            pattern=pattern,
            table_name=table_name,
            write_disposition="replace"
        )
        
        # 4. Verify data
        df = client.table(table_name).collect("pandas")
        assert len(df) == 300  # 3 files * 100 rows each
        assert set(df['batch'].unique()) == {0, 1, 2}
        
    finally:
        # Cleanup temp files
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        # Cleanup Dremio
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
