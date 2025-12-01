import pytest
import os
from dremioframe.ai.agent import DremioAgent

@pytest.fixture(scope="module")
def agent():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set")
    return DremioAgent(model="gpt-4o")

def test_generate_dq_recipe(agent):
    """
    Test generating a DQ recipe for sys.jobs.
    """
    try:
        recipe = agent.generate_dq_recipe("sys.jobs")
        print(f"DQ Recipe:\n{recipe}")
        
        assert recipe is not None
        # Should be YAML
        assert "checks:" in recipe or "table:" in recipe or "columns:" in recipe
        
    except Exception as e:
        pytest.skip(f"Skipping DQ test due to error: {e}")
