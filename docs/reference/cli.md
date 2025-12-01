# DremioFrame CLI

DremioFrame includes a command-line interface (CLI) for quick interaction with Dremio.

## Installation

The CLI is installed automatically with `dremioframe`.

```bash
pip install dremioframe
```

## Interactive Shell (REPL)

DremioFrame provides an interactive shell with syntax highlighting and auto-completion.

```bash
dremio-cli repl
```

Commands:
- `SELECT ...`: Execute SQL query and display results.
- `tables`: List tables in the root catalog.
- `exit` or `quit`: Exit the shell.

Requires `rich` and `prompt_toolkit` (install with `pip install dremioframe[cli]`).

## Configuration

Set the following environment variables:

- `DREMIO_PAT`: Personal Access Token
- `DREMIO_URL`: Dremio URL (e.g., `data.dremio.cloud`)
- `DREMIO_PROJECT_ID`: Project ID (optional, for Cloud)

## Usage

### Run a Query

```bash
dremio-cli query "SELECT * FROM my_table LIMIT 5"
```

### List Catalog

```bash
# List root catalog
dremio-cli catalog

# List specific path
dremio-cli catalog --path "source.folder"
```

### List Reflections

```bash
dremio-cli reflections
```
