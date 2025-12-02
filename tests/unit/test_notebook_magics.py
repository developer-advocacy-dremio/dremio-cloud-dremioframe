import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from dremioframe.notebook import DremioMagics

class MockShell:
    def __init__(self):
        self.user_ns = {}

def test_dremio_connect_magic():
    """Test %dremio_connect magic"""
    shell = MockShell()
    magics = DremioMagics(shell)
    
    with patch('dremioframe.notebook.DremioClient') as MockClient:
        MockClient.return_value.base_url = "https://test.dremio.cloud"
        
        # Test with args
        magics.dremio_connect("pat=test_pat project_id=test_project")
        
        MockClient.assert_called_with(pat="test_pat", project_id="test_project")
        assert 'dremio_client' in shell.user_ns
        assert magics.client is not None

def test_dremio_sql_magic():
    """Test %%dremio_sql magic"""
    shell = MockShell()
    magics = DremioMagics(shell)
    
    # Mock client
    client = MagicMock()
    magics.client = client
    
    # Mock DremioBuilder
    with patch('dremioframe.notebook.DremioBuilder') as MockBuilder:
        mock_builder = MockBuilder.return_value
        df = pd.DataFrame({'a': [1, 2]})
        mock_builder.collect.return_value = df
        
        # Test execution
        result = magics.dremio_sql("", "SELECT * FROM test")
        
        MockBuilder.assert_called_with(client, sql="SELECT * FROM test")
        mock_builder.collect.assert_called_with(library='pandas', progress_bar=True)
        assert result.equals(df)

def test_dremio_sql_magic_with_variable():
    """Test %%dremio_sql magic with variable assignment"""
    shell = MockShell()
    magics = DremioMagics(shell)
    magics.client = MagicMock()
    
    with patch('dremioframe.notebook.DremioBuilder') as MockBuilder:
        mock_builder = MockBuilder.return_value
        df = pd.DataFrame({'a': [1, 2]})
        mock_builder.collect.return_value = df
        
        # Test with variable name
        magics.dremio_sql("my_df", "SELECT * FROM test")
        
        assert 'my_df' in shell.user_ns
        assert shell.user_ns['my_df'].equals(df)
