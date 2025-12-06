# Using MCP Tools in Dremio Agent

> [!NOTE]
> This guide explains how to use external MCP tools *within* the Dremio Agent. If you want to use DremioFrame as an MCP **Server** for other clients (like Claude), see [MCP Server](mcp_server.md).

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

### Transport Protocols

MCP supports three transport protocols for client-server communication. Choose based on your deployment scenario:

#### `stdio` (Standard Input/Output)

**Best for**: Local integrations, command-line tools, development

**How it works**: The client launches the MCP server as a subprocess and communicates through standard input/output streams.

**Characteristics**:
- ✅ **Lowest latency** - No network overhead
- ✅ **Most secure** - No network exposure
- ✅ **Simplest setup** - No network configuration needed
- ❌ **Single client only** - One-to-one relationship
- ❌ **Same machine only** - Cannot communicate across network

**When to use**:
- Local AI tools running on your machine
- Development and testing
- Security-sensitive operations
- Command-line integrations

**Example**:
```python
mcp_servers = {
    "local_files": {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
    }
}
```

---

#### `http` (Streamable HTTP) - **Recommended for Production**

**Best for**: Web applications, remote servers, multi-client environments

**How it works**: MCP server runs as an independent HTTP server. Clients connect over HTTP/HTTPS. Optionally uses Server-Sent Events (SSE) for server-to-client streaming.

**Characteristics**:
- ✅ **Remote access** - Works across networks
- ✅ **Multiple clients** - Supports concurrent connections
- ✅ **Authentication** - Supports JWT, API keys, etc.
- ✅ **Scalable** - Enterprise-ready
- ✅ **Modern standard** - Latest MCP specification
- ❌ **More complex setup** - Requires server deployment

**When to use**:
- Web-based AI applications
- Cloud-hosted MCP servers
- Multiple AI clients accessing same server
- Enterprise integrations requiring authentication
- Production deployments

**Example**:
```python
mcp_servers = {
    "remote_api": {
        "transport": "http",
        "url": "https://mcp-server.example.com",
        "headers": {
            "Authorization": "Bearer YOUR_TOKEN"
        }
    }
}
```

**OAuth 2.0 Support**:

Yes! The `http` transport supports OAuth authentication via the `headers` parameter. You can pass OAuth tokens (Bearer tokens, API keys, etc.) with every request:

```python
mcp_servers = {
    "oauth_server": {
        "transport": "http",
        "url": "https://secure-mcp-server.example.com/mcp",
        "headers": {
            "Authorization": "Bearer YOUR_OAUTH_TOKEN",
            "X-Custom-Header": "additional-auth-data"
        }
    }
}
```

**Note**: The current implementation passes static headers. For OAuth token refresh, you'll need to manage token renewal externally and reinitialize the agent with updated tokens.

---

#### `sse` (Server-Sent Events) - **Legacy/Deprecated**

**Best for**: Backwards compatibility only

**How it works**: Uses Server-Sent Events for server-to-client streaming, paired with HTTP POST for client-to-server requests.

**Status**: ⚠️ **Deprecated** - Replaced by Streamable HTTP

**When to use**:
- Only for compatibility with older MCP servers
- **Not recommended for new implementations**

---

### Quick Comparison

| Feature | stdio | http | sse |
|---------|-------|------|-----|
| **Latency** | Lowest | Medium | Medium |
| **Security** | Highest (local) | Good (with auth) | Good (with auth) |
| **Setup Complexity** | Simplest | Moderate | Moderate |
| **Multi-client** | ❌ No | ✅ Yes | ✅ Yes |
| **Remote Access** | ❌ No | ✅ Yes | ✅ Yes |
| **Authentication** | N/A | ✅ Yes | ✅ Yes |
| **Status** | ✅ Active | ✅ Recommended | ⚠️ Deprecated |

### Configuration Format

Each MCP server configuration requires:
- **transport**: Communication method (`"stdio"` or `"http"`)
- **command** (stdio only): Executable command (e.g., `"npx"`, `"python"`)
- **args** (stdio only): Command arguments (list of strings)
- **url** (http only): Server URL
- **headers** (http only, optional): Authentication headers

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
