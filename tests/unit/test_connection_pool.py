import pytest
from unittest.mock import MagicMock, patch
from dremioframe.connection_pool import ConnectionPool
import time

def test_pool_creation():
    """Test pool initialization"""
    pool = ConnectionPool(max_size=2, pat="test")
    assert pool.max_size == 2
    assert pool.client_kwargs == {"pat": "test"}
    assert pool.pool.empty()
    assert pool.created_count == 0

def test_get_client_creates_new():
    """Test get_client creates new client if pool empty and under limit"""
    with patch('dremioframe.connection_pool.DremioClient') as MockClient:
        pool = ConnectionPool(max_size=2)
        
        client1 = pool.get_client()
        assert pool.created_count == 1
        assert client1 == MockClient.return_value
        
        client2 = pool.get_client()
        assert pool.created_count == 2
        
        MockClient.assert_called()

def test_release_client():
    """Test releasing client returns it to pool"""
    with patch('dremioframe.connection_pool.DremioClient') as MockClient:
        pool = ConnectionPool(max_size=2)
        
        client = pool.get_client()
        assert pool.pool.empty()
        
        pool.release_client(client)
        assert not pool.pool.empty()
        assert pool.pool.qsize() == 1
        
        # Get again should return same client (mock check not easy here as queue returns item)
        client2 = pool.get_client()
        assert client2 == client

def test_pool_limit_and_timeout():
    """Test pool enforces max size and timeouts"""
    with patch('dremioframe.connection_pool.DremioClient'):
        pool = ConnectionPool(max_size=1, timeout=0.1)
        
        client1 = pool.get_client()
        
        # Pool is full (created_count == max_size) and empty (client checked out)
        with pytest.raises(TimeoutError):
            pool.get_client()
            
        # Return client
        pool.release_client(client1)
        
        # Now should succeed
        client2 = pool.get_client()
        assert client2 == client1

def test_context_manager():
    """Test context manager usage"""
    with patch('dremioframe.connection_pool.DremioClient'):
        pool = ConnectionPool(max_size=1)
        
        with pool.client() as client:
            assert client is not None
            assert pool.pool.empty()
            
        # Should be back in pool
        assert not pool.pool.empty()
        assert pool.pool.qsize() == 1
