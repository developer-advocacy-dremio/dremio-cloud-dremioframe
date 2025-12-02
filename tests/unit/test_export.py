import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
from dremioframe.builder import DremioBuilder

def test_to_delta():
    """Test export to Delta Lake"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    df = pd.DataFrame({'a': [1, 2]})
    
    # Mock collect and write_deltalake
    with patch.object(DremioBuilder, 'collect', return_value=df), \
         patch('dremioframe.builder.write_deltalake', create=True) as mock_write: # create=True to mock import
        
        # We need to mock the import inside the method or use sys.modules
        # Since the method does "from deltalake import write_deltalake"
        # We can patch sys.modules['deltalake']
        
        with patch.dict('sys.modules', {'deltalake': MagicMock()}):
             # Mock the imported function
             sys_modules_mock = sys.modules['deltalake']
             sys_modules_mock.write_deltalake = MagicMock()
             
             builder.to_delta("path/to/delta", mode="append")
             
             sys_modules_mock.write_deltalake.assert_called_with(
                 "path/to/delta", df, mode="append"
             )

def test_to_json():
    """Test export to JSON"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    df = pd.DataFrame({'a': [1, 2]})
    
    with patch.object(DremioBuilder, 'collect', return_value=df):
        with patch.object(pd.DataFrame, 'to_json') as mock_to_json:
            builder.to_json("path.json", orient="records")
            
            mock_to_json.assert_called_with("path.json", orient="records")

def test_to_delta_import_error():
    """Test error when deltalake is not installed"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    # Ensure import fails
    with patch.dict('sys.modules', {'deltalake': None}):
        with pytest.raises(ImportError) as exc:
            builder.to_delta("path")
        assert "deltalake package is required" in str(exc.value)
