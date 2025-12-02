import pytest
from unittest.mock import MagicMock
from dremioframe.linter import SqlLinter

def test_validate_sql_success():
    """Test successful SQL validation"""
    client = MagicMock()
    linter = SqlLinter(client)
    
    # Mock successful execution
    client.sql.return_value.collect.return_value = None
    
    result = linter.validate_sql("SELECT 1")
    
    assert result["valid"] is True
    assert result["error"] is None
    client.sql.assert_called_with("EXPLAIN PLAN FOR SELECT 1")

def test_validate_sql_failure():
    """Test failed SQL validation"""
    client = MagicMock()
    linter = SqlLinter(client)
    
    # Mock exception
    client.sql.side_effect = Exception("Syntax error")
    
    result = linter.validate_sql("SELECT * FROM non_existent")
    
    assert result["valid"] is False
    assert "Syntax error" in result["error"]

def test_lint_sql():
    """Test static linting rules"""
    linter = SqlLinter()
    
    # Test SELECT *
    warnings = linter.lint_sql("SELECT * FROM table")
    assert len(warnings) == 1
    assert "Avoid 'SELECT *'" in warnings[0]
    
    # Test DELETE without WHERE
    warnings = linter.lint_sql("DELETE FROM table")
    assert len(warnings) == 1
    assert "DELETE statement missing WHERE" in warnings[0]
    
    # Test clean SQL
    warnings = linter.lint_sql("SELECT id FROM table WHERE id = 1")
    assert len(warnings) == 0
