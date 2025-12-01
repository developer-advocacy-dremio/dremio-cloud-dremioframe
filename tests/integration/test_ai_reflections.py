import pytest
import os
from dremioframe.ai.agent import DremioAgent

@pytest.fixture(scope="module")
def agent():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set")
    return DremioAgent(model="gpt-4o")

def test_recommend_reflections(agent):
    """
    Test that the agent can recommend reflections for a query.
    """
    query = "SELECT region, SUM(sales_amount) as total_sales FROM sales.transactions GROUP BY region"
    recommendation = agent.recommend_reflections(query)
    
    print(f"Reflection Recommendation: {recommendation}")
    
    assert recommendation is not None
    assert len(recommendation) > 10
    # Should suggest aggregation reflection
    assert "aggregation" in recommendation.lower()
    # Should mention the fields
    assert "region" in recommendation.lower()
    assert "sales_amount" in recommendation.lower()

def test_list_reflections(agent):
    """
    Test listing reflections via the agent.
    """
    response = agent.agent.invoke({"messages": [("user", "List all reflections")]})
    output = response["messages"][-1].content
    print(f"Agent Output: {output}")
    
    assert output is not None
    # Should list reflections or say none found
    assert "reflection" in output.lower() or "none" in output.lower() or "id" in output.lower()
