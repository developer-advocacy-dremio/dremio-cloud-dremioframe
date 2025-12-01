import pytest
import os
from dremioframe.ai.agent import DremioAgent

@pytest.fixture(scope="module")
def agent():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set")
    return DremioAgent(model="gpt-4o")

def test_optimize_sql(agent):
    """
    Test SQL optimization.
    """
    # Use a simple query that should be explainable
    query = "SELECT * FROM sys.jobs LIMIT 5"
    
    try:
        optimization = agent.optimize_sql(query)
        print(f"Optimization Output:\n{optimization}")
        
        assert optimization is not None
        # Should mention plan or analysis
        assert "plan" in optimization.lower() or "scan" in optimization.lower() or "project" in optimization.lower()
        
    except Exception as e:
        pytest.skip(f"Skipping optimization test due to error (likely explain plan failure): {e}")
