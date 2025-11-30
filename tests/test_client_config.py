import pytest
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
from unittest.mock import MagicMock, patch

def test_client_flight_config():
    client = DremioClient(
        pat="test", 
        project_id="test", 
        flight_port=12345, 
        flight_endpoint="flight.dremio.com"
    )
    
    assert client.flight_port == 12345
    assert client.flight_endpoint == "flight.dremio.com"
    
    # Verify builder uses it
    builder = DremioBuilder(client, "test_table")
    
    with patch('pyarrow.flight.FlightClient') as mock_flight:
        # Mock get_flight_info and do_get
        mock_client_instance = MagicMock()
        mock_flight.return_value = mock_client_instance
        
        mock_info = MagicMock()
        mock_info.endpoints = [MagicMock()]
        mock_client_instance.get_flight_info.return_value = mock_info
        
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = MagicMock() # Mock table
        mock_client_instance.do_get.return_value = mock_reader
        
        try:
            builder.collect()
        except:
            pass # We expect it might fail on read_all return value conversion, but we check call args
            
        # Check if FlightClient was initialized with correct location
        # location = "grpc+tls://flight.dremio.com:12345"
        mock_flight.assert_called_with("grpc+tls://flight.dremio.com:12345")
