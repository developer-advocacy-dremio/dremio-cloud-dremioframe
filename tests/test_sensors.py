import pytest
from dremioframe.orchestration.sensors import SqlSensor, FileSensor
from dremioframe.client import DremioClient
from unittest.mock import MagicMock
import pandas as pd

def test_sql_sensor_success():
    client = MagicMock(spec=DremioClient)
    # Mock query to return empty first, then data
    client.query.side_effect = [
        pd.DataFrame(), # Empty
        pd.DataFrame({"col": [1]}) # Data
    ]
    
    sensor = SqlSensor("sql_sensor", client, "SELECT 1", poke_interval=0.1)
    result = sensor.run({})
    assert result is True
    assert client.query.call_count == 2

def test_file_sensor_success():
    client = MagicMock(spec=DremioClient)
    mock_builder = MagicMock()
    client.list_files.return_value = mock_builder
    
    # Mock collect
    mock_builder.collect.side_effect = [
        pd.DataFrame(),
        pd.DataFrame({"file": ["a.txt"]})
    ]
    
    sensor = FileSensor("file_sensor", client, "path/to/file", poke_interval=0.1)
    result = sensor.run({})
    assert result is True
    assert mock_builder.collect.call_count == 2
