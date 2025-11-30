import pytest
from unittest.mock import MagicMock, patch
import sys

# Mock prompt_toolkit and pygments before importing repl
mock_pt = MagicMock()
sys.modules["prompt_toolkit"] = mock_pt
sys.modules["prompt_toolkit.history"] = MagicMock()
sys.modules["prompt_toolkit.auto_suggest"] = MagicMock()
sys.modules["prompt_toolkit.lexers"] = MagicMock()
sys.modules["pygments.lexers.sql"] = MagicMock()

from dremioframe.cli import repl
import pandas as pd

def test_repl_query():
    # Mock dependencies
    # We need to patch the PromptSession class that is imported inside repl
    # Since we mocked the module, we can configure the mock
    mock_session_cls = mock_pt.PromptSession
    
    with patch('dremioframe.cli.get_client') as mock_get_client, \
         patch('dremioframe.cli.console') as mock_console:
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        
        # Simulate inputs: "SELECT 1", "exit"
        mock_session.prompt.side_effect = ["SELECT 1", "exit"]
        
        # Mock query result
        mock_client.query.return_value = pd.DataFrame({"val": [1]})
        
        # Run REPL
        repl()
        
        # Verify query called
        mock_client.query.assert_called_with("SELECT 1", format="pandas")
        
        # Verify output printed
        # console.print called with markdown table
        assert mock_console.print.call_count >= 3 # Welcome, Result, Goodbye

def test_repl_tables():
    # Mock dependencies
    mock_session_cls = mock_pt.PromptSession
    
    with patch('dremioframe.cli.get_client') as mock_get_client, \
         patch('dremioframe.cli.console') as mock_console:
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        
        # Simulate inputs: "tables", "exit"
        mock_session.prompt.side_effect = ["tables", "exit"]
        
        # Mock catalog list
        mock_client.catalog.list_catalog.return_value = [{"path": ["source"], "type": "CONTAINER"}]
        
        # Run REPL
        repl()
        
        # Verify catalog called
        mock_client.catalog.list_catalog.assert_called()
