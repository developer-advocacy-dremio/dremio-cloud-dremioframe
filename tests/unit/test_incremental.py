import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from dremioframe.incremental import IncrementalLoader

def test_get_watermark():
    """Test retrieving watermark"""
    client = MagicMock()
    loader = IncrementalLoader(client)
    
    # Mock builder response
    with patch('dremioframe.incremental.DremioBuilder') as MockBuilder:
        mock_builder = MockBuilder.return_value
        df = pd.DataFrame({'max_val': [100]})
        mock_builder.select.return_value.collect.return_value = df
        
        val = loader.get_watermark("test.table", "id")
        
        assert val == 100
        MockBuilder.assert_called_with(client, "test.table")
        mock_builder.select.assert_called_with("MAX(id) as max_val")

def test_get_watermark_empty():
    """Test retrieving watermark from empty table"""
    client = MagicMock()
    loader = IncrementalLoader(client)
    
    with patch('dremioframe.incremental.DremioBuilder') as MockBuilder:
        mock_builder = MockBuilder.return_value
        df = pd.DataFrame() # Empty result
        mock_builder.select.return_value.collect.return_value = df
        
        val = loader.get_watermark("test.table", "id")
        
        assert val is None

def test_load_incremental():
    """Test incremental load logic"""
    client = MagicMock()
    loader = IncrementalLoader(client)
    
    # Mock get_watermark
    loader.get_watermark = MagicMock(return_value="2024-01-01")
    
    # Mock SQL execution
    df = pd.DataFrame({'Rows Inserted': [50]})
    client.sql.return_value.collect.return_value = df
    
    count = loader.load_incremental("source", "target", "ts")
    
    assert count == 50
    # Verify SQL
    expected_sql = "INSERT INTO target SELECT * FROM source WHERE ts > '2024-01-01'"
    client.sql.assert_called_with(expected_sql)

def test_load_incremental_no_watermark():
    """Test incremental load with no existing watermark (full load)"""
    client = MagicMock()
    loader = IncrementalLoader(client)
    
    loader.get_watermark = MagicMock(return_value=None)
    
    df = pd.DataFrame({'Rows Inserted': [100]})
    client.sql.return_value.collect.return_value = df
    
    count = loader.load_incremental("source", "target", "ts")
    
    assert count == 100
    expected_sql = "INSERT INTO target SELECT * FROM source"
    client.sql.assert_called_with(expected_sql)

def test_merge():
    """Test MERGE statement generation"""
    client = MagicMock()
    loader = IncrementalLoader(client)
    
    loader.merge(
        source_table="source",
        target_table="target",
        on=["id"],
        update_cols=["name", "status"],
        insert_cols=["id", "name", "status"]
    )
    
    expected_sql = (
        "MERGE INTO target AS target USING source AS source ON (target.id = source.id) "
        "WHEN MATCHED THEN UPDATE SET name = source.name, status = source.status "
        "WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (source.id, source.name, source.status)"
    )
    
    client.sql.assert_called_with(expected_sql)
