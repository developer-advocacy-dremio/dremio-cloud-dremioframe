import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from dremioframe.builder import DremioBuilder

class MockFlightStream:
    def __init__(self, batches):
        self.batches = batches
        self.schema = MagicMock()
        self.schema.empty_table.return_value = MagicMock()

    def __iter__(self):
        return iter(self.batches)

    def read_all(self):
        return MagicMock()

def test_repr_html():
    """Test _repr_html_ returns HTML string"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    # Mock _execute_flight to return a small dataframe
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    
    # We need to mock _execute_flight on the instance or class
    with patch.object(DremioBuilder, '_execute_flight', return_value=df):
        html = builder._repr_html_()
        
        assert isinstance(html, str)
        assert "<h4>DremioBuilder Preview</h4>" in html
        assert "<table" in html
        assert "test.table" in html  # Should be in the SQL preview

def test_repr_html_error():
    """Test _repr_html_ handles errors gracefully"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    with patch.object(DremioBuilder, '_execute_flight', side_effect=Exception("Flight error")):
        html = builder._repr_html_()
        
        assert "Error generating preview" in html
        assert "Flight error" in html

def test_collect_progress_bar():
    """Test collect with progress_bar=True uses tqdm"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    # Create a real RecordBatch to avoid mocking immutable pyarrow types
    import pyarrow as pa
    batch = pa.RecordBatch.from_arrays([pa.array([1])], names=['a'])
    
    # Mock Flight client and reader
    with patch('pyarrow.flight.FlightClient') as MockClient, \
         patch('tqdm.tqdm') as mock_tqdm:
        
        mock_instance = MockClient.return_value
        mock_reader = MagicMock()
        # Make reader iterable yielding real batches
        mock_reader.__iter__.return_value = [batch, batch]
        mock_instance.do_get.return_value = mock_reader
        
        # We don't need to mock from_batches if we provide real batches
        # But we need to ensure the result can be converted to pandas
        # The real from_batches will return a real Table
        
        result = builder.collect(library='pandas', progress_bar=True)
        
        # Verify tqdm was used
        mock_tqdm.assert_called()
        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

def test_collect_no_progress_bar():
    """Test collect without progress_bar does not use tqdm"""
    client = MagicMock()
    builder = DremioBuilder(client, "test.table")
    
    with patch('pyarrow.flight.FlightClient') as MockClient, \
         patch('tqdm.tqdm') as mock_tqdm:
        
        mock_instance = MockClient.return_value
        mock_reader = MagicMock()
        mock_instance.do_get.return_value = mock_reader
        mock_reader.read_all.return_value = MagicMock()
        
        builder.collect(library='pandas', progress_bar=False)
        
        # Verify tqdm was NOT used
        mock_tqdm.assert_not_called()
        # Verify read_all was called
        mock_reader.read_all.assert_called()
