import pytest
import os
from unittest.mock import MagicMock, patch
from dremioframe.ai.agent import DremioAgent, list_documentation, search_dremio_docs

# --- Unit Tests ---

@pytest.fixture
def mock_agent():
    with patch("dremioframe.ai.agent.ChatOpenAI"), \
         patch("dremioframe.ai.agent.create_react_agent") as MockCreateAgent:
        
        mock_agent_executor = MockCreateAgent.return_value
        agent = DremioAgent(api_key="fake-key")
        # agent.agent is the executor in langgraph
        return agent, mock_agent_executor

def test_agent_initialization():
    with patch("dremioframe.ai.agent.ChatOpenAI") as MockLLM:
        agent = DremioAgent(api_key="fake-key")
        assert agent.model_name == "gpt-4o"
        MockLLM.assert_called()

def test_custom_llm_initialization():
    mock_llm = MagicMock()
    agent = DremioAgent(llm=mock_llm)
    assert agent.llm == mock_llm

def test_generate_script(mock_agent):
    agent, mock_executor = mock_agent
    # Mock response structure for langgraph: {"messages": [..., AIMessage(content="...")]}
    mock_message = MagicMock()
    mock_message.content = "```python\nprint('Hello')\n```"
    mock_executor.invoke.return_value = {"messages": [mock_message]}
    
    script = agent.generate_script("Say hello")
    assert script == "print('Hello')"
    mock_executor.invoke.assert_called()

def test_list_documentation():
    with patch("glob.glob", return_value=["docs/test.md"]), \
         patch("os.path.exists", return_value=True):
        docs = list_documentation.invoke({})
        assert len(docs) > 0

def test_search_dremio_docs():
    with patch("os.walk", return_value=[("/path/to/dremiodocs", [], ["test.md"])]), \
         patch("builtins.open", new_callable=MagicMock) as mock_open, \
         patch("os.path.exists", return_value=True):
        
        mock_file = MagicMock()
        mock_file.read.return_value = "This is a test document about SQL."
        mock_open.return_value.__enter__.return_value = mock_file
        
        results = search_dremio_docs.invoke({"query": "SQL"})
        assert len(results) > 0

# --- Integration Tests ---

@pytest.mark.skipif(not os.environ.get("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
def test_ai_integration_generate_and_run():
    """
    Integration test that uses OpenAI to generate a script and then runs it.
    """
    agent = DremioAgent(model="gpt-4o")
    
    # Prompt to generate a simple script that prints something specific
    prompt = "Write a python script using dremioframe (mocking the client if needed, or just standard python) that prints 'AI Generation Successful'"
    
    output_file = "generated_script.py"
    if os.path.exists(output_file):
        os.remove(output_file)
        
    try:
        agent.generate_script(prompt, output_file)
        
        assert os.path.exists(output_file)
        
        # Run the generated script
        import subprocess
        result = subprocess.run(["python3", output_file], capture_output=True, text=True)
        
        assert result.returncode == 0
        assert "AI Generation Successful" in result.stdout
        
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)
