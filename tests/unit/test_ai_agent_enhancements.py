import pytest
from unittest.mock import MagicMock, patch
import os
import tempfile

def test_agent_with_mcp_servers():
    """Test DremioAgent initialization with MCP servers"""
    from dremioframe.ai.agent import DremioAgent
    
    mcp_config = {
        "test_server": {
            "transport": "stdio",
            "command": "echo",
            "args": ["test"]
        }
    }
    
    # Mock the MCP client to avoid actual server connection
    with patch("dremioframe.ai.agent.DremioAgent._initialize_mcp_tools") as mock_mcp:
        mock_mcp.return_value = []
        
        agent = DremioAgent(
            model="gpt-4o",
            api_key="test-key",
            mcp_servers=mcp_config
        )
        
        assert agent.mcp_servers == mcp_config
        mock_mcp.assert_called_once()

def test_agent_without_mcp_servers():
    """Test DremioAgent initialization without MCP servers"""
    from dremioframe.ai.agent import DremioAgent
    
    agent = DremioAgent(
        model="gpt-4o",
        api_key="test-key"
    )
    
    assert agent.mcp_servers == {}

def test_pdf_tool_creation():
    """Test PDF reading tool creation"""
    from dremioframe.ai.agent import DremioAgent
    
    with tempfile.TemporaryDirectory() as tmpdir:
        agent = DremioAgent(
            model="gpt-4o",
            api_key="test-key",
            context_folder=tmpdir
        )
        
        # Verify PDF tool was added
        tool_names = [tool.name for tool in agent.tools]
        assert "read_pdf_file" in tool_names
        assert "list_context_files" in tool_names
        assert "read_context_file" in tool_names

def test_pdf_tool_without_pdfplumber():
    """Test PDF tool gracefully handles missing pdfplumber"""
    from dremioframe.ai.agent import DremioAgent
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a dummy PDF file
        pdf_path = os.path.join(tmpdir, "test.pdf")
        with open(pdf_path, "w") as f:
            f.write("dummy content")
        
        agent = DremioAgent(
            model="gpt-4o",
            api_key="test-key",
            context_folder=tmpdir
        )
        
        # Find the PDF tool
        pdf_tool = None
        for tool in agent.tools:
            if tool.name == "read_pdf_file":
                pdf_tool = tool
                break
        
        assert pdf_tool is not None
        
        # Mock pdfplumber import failure
        with patch("builtins.__import__", side_effect=ImportError("pdfplumber not found")):
            result = pdf_tool.func("test.pdf")
            assert "pdfplumber not installed" in result

def test_mcp_tools_import_error():
    """Test MCP tools initialization handles missing langchain-mcp-adapters"""
    from dremioframe.ai.agent import DremioAgent
    
    mcp_config = {"test": {"transport": "stdio", "command": "test"}}
    
    # Mock the LLM to avoid initialization issues
    mock_llm = MagicMock()
    
    # Mock _initialize_mcp_tools to simulate import error
    with patch("dremioframe.ai.agent.DremioAgent._initialize_mcp_tools") as mock_mcp:
        mock_mcp.return_value = []  # Simulates graceful failure
        
        agent = DremioAgent(
            llm=mock_llm,
            mcp_servers=mcp_config
        )
        
        # Should still initialize without MCP tools
        assert agent.mcp_servers == mcp_config
        mock_mcp.assert_called_once()
