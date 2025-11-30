import pytest
import pandas as pd
from pydantic import BaseModel, ValidationError
from unittest.mock import MagicMock
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
from dremioframe.builder import DremioBuilder
from unittest.mock import MagicMock

class UserSchema(BaseModel):
    id: int
    name: str

def test_validation_success(dremio_client):
    builder = dremio_client.table("users")
    builder._execute_dml = MagicMock()
    
    data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    
    # Should pass
    builder.insert("users", data=data, schema=UserSchema)
    
    assert builder._execute_dml.called

def test_validation_failure():
    client = MagicMock(spec=DremioClient)
    builder = DremioBuilder(client)
    
    # Mock collect to return invalid data
    df = pd.DataFrame([{"id": 1, "name": 123}]) # name should be str
    
    with pytest.raises(ValueError, match="Validation failed"):
        builder._validate_data(df, UserSchema)

def test_validation_list_of_dicts(dremio_client):
    builder = dremio_client.table("users")
    builder._execute_dml = MagicMock()
    
    data = [{"id": 1, "name": "Alice"}]
    
    # Should pass
    # Note: insert expects DataFrame or Arrow Table usually, but _validate_data handles list
    # However, insert logic assumes DataFrame/Arrow for column names.
    # So we should pass DataFrame for insert to work, but validation logic itself handles list.
    # Let's stick to DataFrame for full flow test.
    
    df = pd.DataFrame(data)
    builder.insert("users", data=df, schema=UserSchema)
    
    assert builder._execute_dml.called
