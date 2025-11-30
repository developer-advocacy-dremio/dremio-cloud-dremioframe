import pytest
from pydantic import BaseModel
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
from unittest.mock import MagicMock
import pandas as pd

class User(BaseModel):
    id: int
    name: str
    active: bool

def test_create_from_model():
    client = MagicMock(spec=DremioClient)
    builder = DremioBuilder(client)
    
    # Mock _execute_dml
    builder._execute_dml = MagicMock()
    
    builder.create_from_model("users", User)
    
    # Check SQL
    # Expect: CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN)
    # Note: dict order is preserved in Python 3.7+
    call_args = builder._execute_dml.call_args[0][0]
    assert "CREATE TABLE users" in call_args
    assert "id INTEGER" in call_args
    assert "name VARCHAR" in call_args
    assert "active BOOLEAN" in call_args

def test_validate_success():
    client = MagicMock(spec=DremioClient)
    builder = DremioBuilder(client)
    
    # Mock collect to return valid data
    df = pd.DataFrame([{"id": 1, "name": "Alice", "active": True}])
    builder.collect = MagicMock(return_value=df)
    
    builder.validate(User)
    # Should not raise

def test_validate_failure():
    client = MagicMock(spec=DremioClient)
    builder = DremioBuilder(client)
    
    # Mock collect to return invalid data
    df = pd.DataFrame([{"id": "not_int", "name": "Alice", "active": True}])
    builder.collect = MagicMock(return_value=df)
    
    with pytest.raises(ValueError, match="Validation failed"):
        builder.validate(User)
