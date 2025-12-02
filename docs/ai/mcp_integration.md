# MCP Server Integration

The DremioAgent supports integration with **Model Context Protocol (MCP)** servers, allowing you to extend the agent with custom tools from any MCP-compatible server.

## What is MCP?

The Model Context Protocol is an open standard introduced by Anthropic that standardizes how AI systems integrate with external tools, systems, and data sources. It provides a universal interface for:
- File system access
- Database connections
- API integrations
- Custom tool implementations

## Installation

```bash
pip install dremioframe[mcp]
```

## Usage

### Basic Example

```python
from dremioframe.ai.agent import DremioAgent

# Configure MCP servers
mcp_servers = {
    "filesystem": {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/path/to/data"]
    },
    "github": {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-github"]
    }
}

# Initialize agent with MCP servers
agent = DremioAgent(
    model="gpt-4o",
    mcp_servers=mcp_servers
)

# The agent now has access to tools from both MCP servers
script = agent.generate_script(
    "List all files in the data directory and create a table from the CSV files"
)
```

### Available MCP Servers

Popular MCP servers include:
- **@modelcontextprotocol/server-filesystem**: File system operations
- **@modelcontextprotocol/server-github**: GitHub API access
- **@modelcontextprotocol/server-postgres**: PostgreSQL database access
- **@modelcontextprotocol/server-sqlite**: SQLite database access

See the [MCP Server Registry](https://github.com/modelcontextprotocol/servers) for more.

### Configuration Format

Each MCP server configuration requires:
- **transport**: Communication method (`"stdio"`, `"http"`, or `"sse"`)
- **command**: Executable command (e.g., `"npx"`, `"python"`)
- **args**: Command arguments (list of strings)

## Requirements

- **Node.js**: Required for npx-based MCP servers
- **langchain-mcp-adapters**: Installed automatically with `pip install dremioframe[mcp]`

## Example: File System Integration

```python
mcp_servers = {
    "files": {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
    }
}

agent = DremioAgent(mcp_servers=mcp_servers)

# Agent can now read files, list directories, etc.
script = agent.generate_script(
    "Read all CSV files in /data and merge them into a single Dremio table"
)
```

## Troubleshooting

### MCP Server Not Found
Ensure Node.js and npx are installed:
```bash
node --version
npx --version
```

### Import Error
If you see "langchain-mcp-adapters not found":
```bash
pip install dremioframe[mcp]
```

### Server Connection Issues
Check server logs and ensure the command/args are correct for your MCP server.
