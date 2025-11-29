import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from dremioframe.async_client import AsyncDremioClient

@pytest.mark.asyncio
async def test_get_catalog_item():
    with patch("aiohttp.ClientSession") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.close = AsyncMock()  # close is async
        mock_session_cls.return_value = mock_session
        
        # Mock the response object
        mock_response = AsyncMock()
        mock_response.json.return_value = {"id": "123"}
        mock_response.raise_for_status = MagicMock() # raise_for_status is sync
        
        # Mock the context manager for session.get()
        mock_get_ctx = MagicMock()
        mock_get_ctx.__aenter__.return_value = mock_response
        mock_get_ctx.__aexit__.return_value = None
        
        mock_session.get.return_value = mock_get_ctx
        
        async with AsyncDremioClient("pat") as client:
            item = await client.get_catalog_item("123")
            assert item == {"id": "123"}
            mock_session.get.assert_called_with("https://api.dremio.cloud/v0/catalog/123")

@pytest.mark.asyncio
async def test_execute_sql():
    with patch("aiohttp.ClientSession") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.close = AsyncMock() # close is async
        mock_session_cls.return_value = mock_session
        
        mock_response = AsyncMock()
        mock_response.json.return_value = {"jobId": "job1"}
        mock_response.raise_for_status = MagicMock() # raise_for_status is sync
        
        mock_post_ctx = MagicMock()
        mock_post_ctx.__aenter__.return_value = mock_response
        mock_post_ctx.__aexit__.return_value = None
        
        mock_session.post.return_value = mock_post_ctx
        
        async with AsyncDremioClient("pat") as client:
            result = await client.execute_sql("SELECT 1")
            assert result == {"jobId": "job1"}
            mock_session.post.assert_called_with(
                "https://api.dremio.cloud/v0/sql",
                json={"sql": "SELECT 1"}
            )
