import pytest
from unittest.mock import MagicMock
from dremioframe.client import DremioClient

@pytest.fixture
def mock_client():
    client = DremioClient(pat="mock_pat")
    client.execute = MagicMock()
    return client

def test_users(mock_client):
    mock_client.admin.create_user("bob", "pass")
    mock_client.execute.assert_called_with("CREATE USER 'bob' PASSWORD 'pass'")
    
    mock_client.admin.alter_user_password("bob", "newpass")
    mock_client.execute.assert_called_with("ALTER USER 'bob' SET PASSWORD 'newpass'")
    
    mock_client.admin.drop_user("bob")
    mock_client.execute.assert_called_with("DROP USER 'bob'")

def test_grants(mock_client):
    mock_client.admin.grant("SELECT", "TABLE t", to_role="r")
    mock_client.execute.assert_called_with("GRANT SELECT ON TABLE t TO ROLE 'r'")
    
    mock_client.admin.revoke("ALL", "SPACE s", from_user="u")
    mock_client.execute.assert_called_with("REVOKE ALL ON SPACE s FROM USER 'u'")

def test_policies(mock_client):
    # Create Function
    mock_client.admin.create_policy_function("f", "x INT", "INT", "x+1")
    mock_client.execute.assert_called_with("CREATE FUNCTION f (x INT) RETURNS INT RETURN x+1")
    
    # Masking
    mock_client.admin.apply_masking_policy("t", "c", "f(c)")
    mock_client.execute.assert_called_with("ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY f(c)")
    
    mock_client.admin.drop_masking_policy("t", "c")
    mock_client.execute.assert_called_with("ALTER TABLE t MODIFY COLUMN c UNSET MASKING POLICY")
    
    # Row Access
    mock_client.admin.apply_row_access_policy("t", "f(c)")
    mock_client.execute.assert_called_with("ALTER TABLE t ADD ROW ACCESS POLICY f(c)")
    
    mock_client.admin.drop_row_access_policy("t", "p")
    mock_client.execute.assert_called_with("ALTER TABLE t DROP ROW ACCESS POLICY p")
