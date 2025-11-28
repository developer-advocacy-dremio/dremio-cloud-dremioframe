import pytest
import pandas as pd
import polars as pl
import uuid
import time
from dremioframe.client import DremioClient

# Use a unique folder name for this test run
TEST_ID = uuid.uuid4().hex[:8]
TEST_FOLDER = f"test_run_{TEST_ID}"
NAMESPACE = "testing"
FULL_FOLDER_PATH = [NAMESPACE, TEST_FOLDER]

# Table names within the folder
TABLE_NAME = f'"{NAMESPACE}"."{TEST_FOLDER}"."integration_table"'
SIMPLE_TABLE = f'"{NAMESPACE}"."{TEST_FOLDER}"."simple_table"'

# Source table to read from (using columns that exist: tripduration, usertype)
READ_TABLE = '"dremio_samples"."nyc_citibike"."citibike"'

@pytest.fixture(scope="module")
def client():
    return DremioClient()

@pytest.fixture(scope="module", autouse=True)
def setup_teardown(client):
    # Setup: Create folder in "testing" namespace
    print(f"Creating folder {NAMESPACE}.{TEST_FOLDER}...")
    try:
        client.catalog.create_folder(FULL_FOLDER_PATH)
    except Exception as e:
        print(f"Warning: Failed to create folder (might exist): {e}")

    yield

    # Teardown: Delete the folder (and everything inside)
    print(f"Cleaning up folder {NAMESPACE}.{TEST_FOLDER}...")
    try:
        # Need to find ID of the folder to delete it
        # List "testing" to find the folder
        items = client.catalog.list_catalog(NAMESPACE)
        folder_id = None
        for item in items:
            if item['path'][-1] == TEST_FOLDER:
                folder_id = item['id']
                break
        
        if folder_id:
            # Delete children first
            try:
                folder_details = client.catalog.get_entity_by_id(folder_id)
                if 'children' in folder_details:
                    for child in folder_details['children']:
                        # Construct path string
                        # child['path'] might include project name "firstproject" at index 0
                        # We should remove it for SQL if it's there, as SQL usually expects Space.Folder.Table
                        p = child['path']
                        if p[0] == "firstproject":
                            p = p[1:]
                        
                        child_path = ".".join([f'"{part}"' for part in p])
                        try:
                            client.execute(f"DROP TABLE {child_path}")
                            print(f"Dropped {child_path}")
                        except Exception as drop_err:
                            print(f"Failed to drop table {child_path}: {drop_err}")
                            # Fallback to API delete if it's not a table (e.g. folder)
                            try:
                                client.catalog.delete_catalog_item(child['id'])
                            except:
                                pass
            except Exception as e:
                print(f"Error deleting children: {e}")
            
            # Now delete the folder
            client.catalog.delete_catalog_item(folder_id)
            print("Folder deleted.")
    except Exception as e:
        print(f"Failed to cleanup folder: {e}")

def test_read_and_mutate(client):
    print(f"Reading from {READ_TABLE}...")
    # 'bikeid' was missing, using 'usertype' instead
    df = (
        client.table(READ_TABLE)
        .select("tripduration", "usertype")
        .mutate(duration_min="tripduration / 60")
        .limit(5)
        .collect()
    )
    print(df)
    assert len(df) == 5
    assert "duration_min" in df.columns

def test_create_table(client):
    print(f"Creating table {TABLE_NAME}...")
    # Create table with 5 rows from source
    client.table(READ_TABLE).limit(5).create(TABLE_NAME)
    
    # Verify it exists
    df = client.table(TABLE_NAME).collect()
    assert len(df) == 5

def test_insert_data(client):
    print(f"Inserting data into {SIMPLE_TABLE}...")
    
    # Create a simple table using CTAS
    client.sql("SELECT 1 as id, 'test' as name").create(SIMPLE_TABLE)
    
    # Now insert
    new_data = pd.DataFrame({"id": [2], "name": ["inserted"]})
    client.table(SIMPLE_TABLE).insert(SIMPLE_TABLE, data=new_data)
    
    # Verify
    df = client.table(SIMPLE_TABLE).collect()
    # Should have 2 rows now
    assert len(df) == 2
    assert 2 in df["id"].to_list()

def test_merge_and_batching(client):
    # Create a target table
    TARGET_TABLE = f'"{NAMESPACE}"."{TEST_FOLDER}"."merge_target"'
    client.sql("SELECT 1 as id, 'original' as val").create(TARGET_TABLE)
    
    # 1. Test Batched Insert
    print(f"Testing batched insert into {TARGET_TABLE}...")
    # Insert 5 rows with batch_size=2 -> 3 batches
    new_data = pd.DataFrame({
        "id": [2, 3, 4, 5, 6], 
        "val": ["v2", "v3", "v4", "v5", "v6"]
    })
    client.table(TARGET_TABLE).insert(TARGET_TABLE, data=new_data, batch_size=2)
    
    df = client.table(TARGET_TABLE).collect()
    assert len(df) == 6 # 1 original + 5 inserted
    
    # 2. Test Merge (Upsert)
    print(f"Testing merge into {TARGET_TABLE}...")
    # Upsert: Update id=1, Insert id=7
    upsert_data = pd.DataFrame({
        "id": [1, 7],
        "val": ["updated", "v7"]
    })
    
    client.table(TARGET_TABLE).merge(
        target_table=TARGET_TABLE,
        on="id",
        matched_update={"val": "source.val"},
        not_matched_insert={"id": "source.id", "val": "source.val"},
        data=upsert_data
    )
    
    # Verify
    df = client.table(TARGET_TABLE).collect()
    assert len(df) == 7 # 6 + 1 inserted
    
    # Check update
    row1 = df.filter(pl.col("id") == 1).to_dicts()[0]
    assert row1["val"] == "updated"
    
    # Check insert
    row7 = df.filter(pl.col("id") == 7).to_dicts()[0]
    assert row7["val"] == "v7"
    
    # Cleanup
    try:
        client.execute(f"DROP TABLE {TARGET_TABLE}")
    except:
        pass

def test_quality_check(client):
    # Run check on the main integration table
    print(f"Running quality check on {TABLE_NAME}...")
    # It should have data
    assert client.table(TABLE_NAME).quality.expect_not_null("tripduration")

def test_custom_quality_check(client):
    print(f"Running custom quality check on {TABLE_NAME}...")
    # We know we inserted 5 rows in test_create_table
    # Let's check that count of rows where tripduration > 0 is 5
    assert client.table(TABLE_NAME).quality.expect_row_count("tripduration > 0", 5, "eq")
    
    # Check that we have 0 rows where tripduration < 0
    assert client.table(TABLE_NAME).quality.expect_row_count("tripduration < 0", 0, "eq")
