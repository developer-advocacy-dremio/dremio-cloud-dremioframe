# Orchestration CLI

DremioFrame provides a CLI to manage your pipelines and orchestration server.

## Installation

The CLI is installed with `dremioframe`.

```bash
dremio-cli --help
```

## Pipeline Commands

### List Pipelines (Runs)

List recent pipeline runs from the backend.

```bash
# Default (SQLite)
dremio-cli pipeline list

# Custom SQLite path
dremio-cli pipeline list --backend-url sqlite:///path/to/db.sqlite

# Postgres
dremio-cli pipeline list --backend-url postgresql://user:pass@host/db
```

### Start UI

Start the Orchestration Web UI.

```bash
dremio-cli pipeline ui --port 8080 --backend-url sqlite:///dremioframe.db
```

## Environment Variables

You can also configure the backend via environment variables if supported by the specific backend class, but the CLI currently relies on the `--backend-url` for instantiation logic.
