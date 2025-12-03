import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from dremioframe.cost_estimator import CostEstimator, CostEstimate, OptimizationHint

def test_estimate_query_cost():
    """Test basic cost estimation"""
    client = MagicMock()
    client.base_url = "http://localhost:9047"
    client.headers = {"Authorization": "Bearer test"}
    estimator = CostEstimator(client)
    
    # Mock REST API response
    plan_text = """
    00-00    Screen
    00-01      Project(EXPR$0=[$0])
    00-02        Scan(table=[[test, table]], rowcount=1000.0, cost={100.0})
    """
    
    with patch('requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'rows': [{'PLAN': plan_text}]
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response
        
        estimate = estimator.estimate_query_cost("SELECT * FROM test.table")
    
    assert isinstance(estimate, CostEstimate)
    assert estimate.estimated_rows == 1000
    assert estimate.total_cost > 0
    assert "scan" in estimate.plan_summary.lower()

def test_parse_plan_metrics():
    """Test plan metrics parsing"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    plan_text = """
    HashJoin(condition=[=($0, $1)], rowcount=5000.0, cost={250.0})
      Scan(table=[[test, table1]], rowcount=1000.0)
      Scan(table=[[test, table2]], rowcount=2000.0)
    """
    
    metrics = estimator._parse_plan_metrics(plan_text)
    
    assert metrics['rows'] == 5000
    assert metrics['scan_cost'] > 0
    assert metrics['join_cost'] > 0

def test_optimization_hints_select_star():
    """Test hint for SELECT *"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    sql = "SELECT * FROM test.table"
    hints = estimator.get_optimization_hints(sql, "Scan(table=[[test, table]])")
    
    hint_messages = [h.message for h in hints]
    assert any("SELECT *" in msg for msg in hint_messages)

def test_optimization_hints_missing_where():
    """Test hint for missing WHERE clause"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    sql = "SELECT col1 FROM test.table"
    plan_text = "Scan(table=[[test, table]], rowcount=1000000.0)"
    hints = estimator.get_optimization_hints(sql, plan_text)
    
    hint_messages = [h.message for h in hints]
    assert any("filter" in msg.lower() or "WHERE" in msg for msg in hint_messages)

def test_optimization_hints_order_without_limit():
    """Test hint for ORDER BY without LIMIT"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    sql = "SELECT col1 FROM test.table ORDER BY col1"
    hints = estimator.get_optimization_hints(sql, "Sort()")
    
    hint_messages = [h.message for h in hints]
    assert any("LIMIT" in msg for msg in hint_messages)

def test_compare_queries():
    """Test query comparison"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    # Mock different costs for different queries
    call_count = [0]
    def mock_sql(sql):
        mock_result = MagicMock()
        if "WHERE" in sql:
            plan_text = "Scan(table=[[test, table]], rowcount=100.0, cost={10.0})"
        else:
            plan_text = "Scan(table=[[test, table]], rowcount=10000.0, cost={1000.0})"
        mock_result.collect.return_value = pd.DataFrame({'PLAN': [plan_text]})
        call_count[0] += 1
        return mock_result
    
    client.sql.side_effect = mock_sql
    
    result = estimator.compare_queries(
        "SELECT * FROM test.table",
        "SELECT * FROM test.table WHERE id > 100"
    )
    
    assert 'queries' in result
    assert 'recommendation' in result
    assert len(result['queries']) == 2
    assert result['best_query_id'] is not None

def test_summarize_plan():
    """Test plan summarization"""
    client = MagicMock()
    estimator = CostEstimator(client)
    
    plan_text = """
    HashJoin
      Scan(table=[[test, table1]])
      Scan(table=[[test, table2]])
    Filter(condition=[>($0, 100)])
    Sort(sort0=[$0])
    """
    
    summary = estimator._summarize_plan(plan_text)
    
    assert "scan" in summary.lower()
    assert "join" in summary.lower()
    assert "filter" in summary.lower()
    assert "sort" in summary.lower()
