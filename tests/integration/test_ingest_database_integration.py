import pytest
import os
import uuid
import pandas as pd
import sqlite3
from dremioframe.client import DremioClient

# Skip if no credentials
@pytest.mark.skipif(not os.getenv("DREMIO_PAT"), reason="No Dremio credentials")
def test_database_integration_live():
    # 1. Setup Client
    client = DremioClient(
        pat=os.getenv("DREMIO_PAT"),
        project_id=os.getenv("DREMIO_PROJECT_ID")
    )
    
    # Setup unique folder
    NAMESPACE = "testing"
    TEST_ID = uuid.uuid4().hex[:8]
    TEST_FOLDER = f"db_test_{TEST_ID}"
    FULL_FOLDER_PATH = [NAMESPACE, TEST_FOLDER]
    
    # Create folder
    try:
        client.catalog.create_folder(FULL_FOLDER_PATH)
    except Exception as e:
        pytest.fail(f"Failed to create test folder: {e}")

    # 2. Setup Local SQLite DB
    db_path = f"test_db_{TEST_ID}.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    conn.commit()
    conn.close()
    
    # 3. Ingest
    table_name = f'"{NAMESPACE}"."{TEST_FOLDER}"."users"'
    connection_string = f"sqlite:///{db_path}"
    
    try:
        # Run ingestion (using sqlalchemy backend as connectorx might need setup)
        # We need sqlalchemy installed. It should be if we ran pip install .[database]
        # But for test env, we might need to ensure it.
        
        client.ingest.database(
            connection_string=connection_string,
            query="SELECT * FROM users",
            table_name=table_name,
            backend="sqlalchemy",
            write_disposition="replace"
        )
        
        # 4. Verify
        df = client.table(table_name).collect("pandas")
        
        assert len(df) == 2
        assert "Alice" in df['name'].values
        
    finally:
        # Cleanup Dremio
        try:
            # Find folder ID to delete
            items = client.catalog.list_catalog(NAMESPACE)
            folder_id = None
            for item in items:
                if item['path'][-1] == TEST_FOLDER:
                    folder_id = item['id']
                    break
            
            if folder_id:
                client.catalog.delete_catalog_item(folder_id)
        except Exception as e:
            print(f"Dremio cleanup failed: {e}")
            
        # Cleanup Local DB
        if os.path.exists(db_path):
            os.remove(db_path)
