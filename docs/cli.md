# DremioFrame CLI

DremioFrame includes a command-line interface (CLI) for quick interaction with Dremio.

## Installation

The CLI is installed automatically with `dremioframe`.

```bash
pip install dremioframe
```

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
