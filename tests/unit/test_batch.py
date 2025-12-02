import pytest
from unittest.mock import MagicMock
from dremioframe.batch import BatchManager

def test_create_folders():
    """Test batch folder creation"""
    client = MagicMock()
    manager = BatchManager(client, max_workers=2)
    
    # Mock catalog.create_folder
    client.catalog.create_folder.side_effect = lambda path: {"id": path, "path": path}
    
    paths = ["space.f1", "space.f2", "space.f3"]
    results = manager.create_folders(paths)
    
    assert len(results) == 3
    assert results["space.f1"] == {"id": "space.f1", "path": "space.f1"}
    assert client.catalog.create_folder.call_count == 3

def test_delete_items():
    """Test batch item deletion"""
    client = MagicMock()
    manager = BatchManager(client, max_workers=2)
    
    # Mock catalog.delete
    client.catalog.delete.return_value = None
    
    ids = ["id1", "id2"]
    results = manager.delete_items(ids)
    
    assert len(results) == 2
    assert results["id1"] is True
    assert results["id2"] is True
    assert client.catalog.delete.call_count == 2

def test_batch_error_handling():
    """Test error handling in batch operations"""
    client = MagicMock()
    manager = BatchManager(client, max_workers=2)
    
    # Mock create_folder to fail for one item
    def side_effect(path):
        if path == "fail":
            raise ValueError("Failed")
        return {"id": path}
    
    client.catalog.create_folder.side_effect = side_effect
    
    paths = ["ok", "fail"]
    results = manager.create_folders(paths)
    
    assert results["ok"] == {"id": "ok"}
    assert "error" in results["fail"]
    assert "Failed" in results["fail"]["error"]
