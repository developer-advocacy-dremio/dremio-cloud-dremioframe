import pytest
from unittest.mock import MagicMock, patch
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
import pyarrow.flight as flight

import os

@pytest.fixture
def software_client(monkeypatch):
    monkeypatch.delenv("DREMIO_PAT", raising=False)
    return DremioClient(
        hostname="localhost",
        port=32010,
        username="admin",
        password="password123",
        tls=False,
        pat=None, # Explicitly None, though env var is the issue
        mode="v26" # Explicitly set software mode
    )

def test_software_connection_params(software_client):
    assert software_client.hostname == "localhost"
    assert software_client.port == 32010
    assert software_client.username == "admin"
    assert software_client.password == "password123"
    assert software_client.tls is False
    assert software_client.base_url == "http://localhost:32010/api/v3"

import pyarrow as pa

@patch("pyarrow.flight.FlightClient")
def test_flight_connection_software(mock_flight_client, software_client):
    # Setup Mock
    mock_instance = MagicMock()
    mock_flight_client.return_value = mock_instance
    
    # Mock authenticate_basic_token to return options
    mock_options = MagicMock()
    mock_instance.authenticate_basic_token.return_value = mock_options
    
    # Mock do_get
    mock_reader = MagicMock()
    # Return a real empty table
    real_table = pa.Table.from_pydict({"a": [1]})
    mock_reader.read_all.return_value = real_table
    mock_instance.do_get.return_value = mock_reader
    
    # Execute
    builder = DremioBuilder(software_client, "sys.version")
    builder.collect()
    
    # Verify FlightClient init
    from unittest.mock import ANY
    mock_flight_client.assert_called_with("grpc+tcp://localhost:32010", middleware=ANY)
    
    # Verify Authentication
    mock_instance.authenticate_basic_token.assert_called_with("admin", "password123")
    
    # Verify do_get called with options from auth
    args, kwargs = mock_instance.do_get.call_args
    assert args[1] == mock_options

@patch("pyarrow.flight.FlightClient")
def test_flight_connection_cloud(mock_flight_client):
    # Setup Cloud Client
    client = DremioClient(pat="test_pat")
    
    # Setup Mock
    mock_instance = MagicMock()
    mock_flight_client.return_value = mock_instance
    
    # Mock do_get
    mock_reader = MagicMock()
    real_table = pa.Table.from_pydict({"a": [1]})
    mock_reader.read_all.return_value = real_table
    mock_instance.do_get.return_value = mock_reader
    
    # Execute
    builder = DremioBuilder(client, "sys.version")
    builder.collect()
    
    # Verify FlightClient init
    from unittest.mock import ANY
    mock_flight_client.assert_called_with("grpc+tls://data.dremio.cloud:443", middleware=ANY)
    
    # Verify Headers (PAT)
    # do_get(ticket, options)
    args, kwargs = mock_instance.do_get.call_args
    options = args[1]
    # We can't easily inspect FlightCallOptions in mock without more work, 
    # but we can assume if it didn't call authenticate_basic_token, it used the headers path.
    mock_instance.authenticate_basic_token.assert_not_called()
