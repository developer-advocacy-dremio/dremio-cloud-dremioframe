# Dremio Agent MCP Server

The Dremio Agent MCP Server exposes Dremio capabilities via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing you to use Dremio tools directly within MCP-compliant AI clients like Claude Desktop or IDE extensions.

> [!NOTE]
> This guide explains how to use DremioFrame as an MCP **Server**. If you want to use external MCP tools *within* the Dremio Agent, see [Using MCP Tools](mcp_client.md).

## Installation

The MCP server requires the `mcp` optional dependency:

```bash
pip install "dremioframe[server]"
```

## Configuration

To use the server, you need to configure your MCP client to run the `dremio-cli mcp start` command.

You can generate the configuration JSON using the CLI:

```bash
dremio-cli mcp config
```

This will output a JSON structure similar to:

```json
{
  "mcpServers": {
    "dremio-agent": {
      "command": "/path/to/python",
      "args": [
        "-m",
        "dremioframe.cli",
        "mcp",
        "start"
      ],
      "env": {
        "DREMIO_PAT": "your_pat_here",
        "DREMIO_PROJECT_ID": "your_project_id_here"
      }
    }
  }
}
```

### Environment Variables

Ensure you set the correct environment variables in the `env` section of the configuration:

- **Dremio Cloud**: `DREMIO_PAT`, `DREMIO_PROJECT_ID`
- **Dremio Software**: `DREMIO_SOFTWARE_HOST`, `DREMIO_SOFTWARE_PAT` (or `DREMIO_SOFTWARE_USER`/`PASSWORD`)

## Available Tools

The server exposes the following tools to the AI model:

- **`list_catalog(path)`**: List contents of the catalog.
- **`get_entity(path)`**: Get details of a dataset or container.
- **`query_dremio(sql)`**: Execute a SQL query and get results as JSON.
- **`list_reflections()`**: List all reflections.
- **`get_job_profile(job_id)`**: Get details of a job.
- **`create_view(path, sql)`**: Create a virtual dataset.
- **`list_available_docs()`**: List available documentation files.

## Resources

The server exposes documentation as MCP Resources. The AI client can read these resources to understand how to use `dremioframe` and Dremio SQL.

- **`dremio://docs/{category}/{file}`**: Read library documentation.
- **`dremio://dremiodocs/{category}/{file}`**: Read Dremio native documentation.

## Usage Example (Claude Desktop)

1. **Configure**: Add the server config to your Claude Desktop configuration.
2. **Ask**: "How do I query a table using dremioframe?" or "Write a script to list all reflections."
3. **Context**: Claude will use the `list_available_docs` tool to find relevant documentation, read it via the Resources, and then generate the code for you.
