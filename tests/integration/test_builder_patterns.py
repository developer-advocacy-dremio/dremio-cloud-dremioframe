import pytest
import pandas as pd
import uuid
from dremioframe.client import DremioClient

@pytest.fixture
def dremio_client():
    return DremioClient()

@pytest.fixture
def unique_table_name():
    return f"testing.builder_pattern_{uuid.uuid4().hex[:8]}"

def test_builder_create_pattern(dremio_client, unique_table_name):
    """
    Test client.table("name").create("name", data=df) pattern.
    The user finds it redundant to specify the name twice.
    We want to see if it works, and if the first name matters when data is provided.
    """
    try:
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        
        # Pattern: client.table(name).create(name, data=df)
        # We use a dummy name for the builder source to prove it doesn't matter
        print(f"Creating table {unique_table_name} using builder pattern...")
        
        # Using "dummy" as source to show it's ignored when data is passed
        dremio_client.table("dummy_source").create(unique_table_name, data=df)
        
        # Verify
        result = dremio_client.table(unique_table_name).collect()
        assert len(result) == 2
        print("✅ Create pattern successful")
        
    finally:
        dremio_client.execute(f"DROP TABLE IF EXISTS {unique_table_name}")

def test_builder_insert_pattern(dremio_client, unique_table_name):
    """
    Test client.table("name").insert("name", data=df) pattern.
    """
    try:
        # First create the table
        dremio_client.create_table(unique_table_name, {"id": "INTEGER", "val": "VARCHAR"})
        
        df = pd.DataFrame({"id": [3, 4], "val": ["c", "d"]})
        
        print(f"Inserting into {unique_table_name} using builder pattern...")
        
        # Pattern: client.table(name).insert(name, data=df)
        # Again, using "dummy" to show source is ignored
        dremio_client.table("dummy_source").insert(unique_table_name, data=df)
        
        # Verify
        result = dremio_client.table(unique_table_name).collect()
        assert len(result) == 2
        print("✅ Insert pattern successful")
        
    finally:
        dremio_client.execute(f"DROP TABLE IF EXISTS {unique_table_name}")
