import pytest
from unittest.mock import MagicMock
import pandas as pd
from dremioframe.schema_evolution import SchemaManager

def test_get_table_schema():
    """Test retrieving table schema"""
    client = MagicMock()
    manager = SchemaManager(client)
    
    # Mock SQL response for DESCRIBE
    df = pd.DataFrame({
        'Column': ['id', 'name'],
        'Type': ['INT', 'VARCHAR']
    })
    client.sql.return_value.collect.return_value = df
    
    schema = manager.get_table_schema("test.table")
    
    assert schema == {'id': 'INT', 'name': 'VARCHAR'}
    client.sql.assert_called_with("DESCRIBE test.table")

def test_compare_schemas():
    """Test schema comparison logic"""
    client = MagicMock()
    manager = SchemaManager(client)
    
    current = {'id': 'INT', 'name': 'VARCHAR'}
    new = {'id': 'INT', 'name': 'VARCHAR', 'age': 'INT'} # Added age
    
    diff = manager.compare_schemas(current, new)
    assert diff['added_columns'] == {'age': 'INT'}
    assert diff['removed_columns'] == {}
    assert diff['changed_columns'] == {}
    
    # Test removed
    new_removed = {'id': 'INT'}
    diff = manager.compare_schemas(current, new_removed)
    assert diff['removed_columns'] == {'name': 'VARCHAR'}
    
    # Test changed
    new_changed = {'id': 'BIGINT', 'name': 'VARCHAR'}
    diff = manager.compare_schemas(current, new_changed)
    assert diff['changed_columns'] == {'id': {'old_type': 'INT', 'new_type': 'BIGINT'}}

def test_generate_migration_sql():
    """Test SQL generation"""
    client = MagicMock()
    manager = SchemaManager(client)
    
    diff = {
        'added_columns': {'age': 'INT'},
        'removed_columns': {'old_col': 'VARCHAR'},
        'changed_columns': {'id': {'old_type': 'INT', 'new_type': 'BIGINT'}}
    }
    
    sqls = manager.generate_migration_sql("test.table", diff)
    
    assert "ALTER TABLE test.table ADD COLUMN age INT" in sqls
    assert "ALTER TABLE test.table DROP COLUMN old_col" in sqls
    assert "ALTER TABLE test.table ALTER COLUMN id BIGINT" in sqls

def test_sync_table():
    """Test sync_table execution"""
    client = MagicMock()
    manager = SchemaManager(client)
    
    # Mock current schema
    df = pd.DataFrame({
        'Column': ['id'],
        'Type': ['INT']
    })
    client.sql.return_value.collect.return_value = df
    
    new_schema = {'id': 'INT', 'name': 'VARCHAR'}
    
    # Dry run
    sqls = manager.sync_table("test.table", new_schema, dry_run=True)
    assert len(sqls) == 1
    assert "ADD COLUMN name VARCHAR" in sqls[0]
    # Verify no execution
    assert client.sql.call_count == 1 # Only DESCRIBE call
    
    # Real run
    client.sql.reset_mock()
    client.sql.return_value.collect.return_value = df # Reset mock return
    
    manager.sync_table("test.table", new_schema, dry_run=False)
    
    # Should call DESCRIBE and ALTER
    assert client.sql.call_count == 2
    client.sql.assert_any_call("DESCRIBE test.table")
    client.sql.assert_any_call("ALTER TABLE test.table ADD COLUMN name VARCHAR")
