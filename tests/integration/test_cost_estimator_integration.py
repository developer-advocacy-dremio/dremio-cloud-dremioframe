import pytest
import os
from dremioframe.client import DremioClient
from dremioframe.cost_estimator import CostEstimator

@pytest.fixture
def client():
    """Create a real Dremio client for integration testing"""
    return DremioClient()

def test_estimate_real_query_cost(client):
    """Test cost estimation with a real query"""
    estimator = CostEstimator(client)
    
    # Simple query against system table
    sql = 'SELECT * FROM sys.version'
    
    estimate = estimator.estimate_query_cost(sql)
    
    # Verify we got reasonable results
    assert estimate is not None
    assert estimate.estimated_rows >= 0
    assert estimate.total_cost >= 0
    assert len(estimate.plan_summary) > 0
    print(f"\\nCost estimate: {estimate.total_cost}")
    print(f"Plan summary: {estimate.plan_summary}")

def test_optimization_hints_real_query(client):
    """Test optimization hints with real queries"""
    estimator = CostEstimator(client)
    
    # Query with SELECT * (should trigger hint)
    sql = 'SELECT * FROM sys.version'
    estimate = estimator.estimate_query_cost(sql)
    
    # Should have at least one hint about SELECT *
    assert len(estimate.optimization_hints) > 0
    print(f"\\nOptimization hints: {estimate.optimization_hints}")

def test_compare_real_queries(client):
    """Test comparing multiple query variations"""
    estimator = CostEstimator(client)
    
    # Compare two variations of the same query
    result = estimator.compare_queries(
        'SELECT * FROM sys.version',
        'SELECT version FROM sys.version'
    )
    
    assert 'queries' in result
    assert len(result['queries']) == 2
    assert result['best_query_id'] is not None
    print(f"\\nComparison result: {result['recommendation']}")
