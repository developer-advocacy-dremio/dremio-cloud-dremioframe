import pytest
import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch

# Test AI Memory Persistence
def test_ai_agent_memory_persistence():
    """Test that agent can persist and resume conversations."""
    try:
        from dremioframe.ai.agent import DremioAgent
    except ImportError:
        pytest.skip("AI dependencies not installed")
    
    # Create a temporary database for memory
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        memory_path = tmp.name
    
    try:
        # Mock the LLM to avoid actual API calls
        with patch("dremioframe.ai.agent.ChatOpenAI") as MockLLM:
            mock_llm_instance = MagicMock()
            MockLLM.return_value = mock_llm_instance
            
            # Create agent with memory
            agent = DremioAgent(model="gpt-4o", api_key="fake_key", memory_path=memory_path)
            
            # Verify checkpointer was initialized
            assert agent.checkpointer is not None or agent.memory_path == memory_path
            
    finally:
        # Cleanup
        if os.path.exists(memory_path):
            os.unlink(memory_path)

def test_ai_agent_context_folder():
    """Test that agent can access context folder files."""
    try:
        from dremioframe.ai.agent import DremioAgent
    except ImportError:
        pytest.skip("AI dependencies not installed")
    
    # Create a temporary context folder
    context_dir = tempfile.mkdtemp()
    
    try:
        # Create some test files
        with open(os.path.join(context_dir, "schema.sql"), "w") as f:
            f.write("CREATE TABLE users (id INT, name VARCHAR);")
        
        with open(os.path.join(context_dir, "README.md"), "w") as f:
            f.write("# Project Documentation")
        
        # Mock the LLM
        with patch("dremioframe.ai.agent.ChatOpenAI") as MockLLM:
            mock_llm_instance = MagicMock()
            MockLLM.return_value = mock_llm_instance
            
            # Create agent with context folder
            agent = DremioAgent(model="gpt-4o", api_key="fake_key", context_folder=context_dir)
            
            # Verify context tools were added
            tool_names = [tool.name for tool in agent.tools]
            assert "list_context_files" in tool_names
            assert "read_context_file" in tool_names
            
            # Test list_context_files tool
            list_tool = next(t for t in agent.tools if t.name == "list_context_files")
            files = list_tool.func()
            assert "schema.sql" in files
            assert "README.md" in files
            
            # Test read_context_file tool
            read_tool = next(t for t in agent.tools if t.name == "read_context_file")
            content = read_tool.func("schema.sql")
            assert "CREATE TABLE users" in content
            
    finally:
        # Cleanup
        shutil.rmtree(context_dir)

def test_ai_agent_session_id():
    """Test that session_id is passed correctly to agent methods."""
    try:
        from dremioframe.ai.agent import DremioAgent
    except ImportError:
        pytest.skip("AI dependencies not installed")
    
    with patch("dremioframe.ai.agent.ChatOpenAI") as MockLLM:
        mock_llm_instance = MagicMock()
        MockLLM.return_value = mock_llm_instance
        
        # Mock the agent's invoke method
        with patch("dremioframe.ai.agent.create_react_agent") as MockAgent:
            mock_agent_instance = MagicMock()
            mock_agent_instance.invoke.return_value = {
                "messages": [MagicMock(content="```python\nprint('hello')\n```")]
            }
            MockAgent.return_value = mock_agent_instance
            
            agent = DremioAgent(model="gpt-4o", api_key="fake_key")
            
            # Call with session_id
            agent.generate_script("test script", session_id="session123")
            
            # Verify invoke was called with config containing thread_id
            call_args = mock_agent_instance.invoke.call_args
            # Check kwargs (call_args[1]) instead of args (call_args[0])
            assert call_args[1]["config"]["configurable"]["thread_id"] == "session123"
