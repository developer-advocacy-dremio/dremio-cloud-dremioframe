# Optional Dependencies

DremioFrame is designed to be lightweight, with a core set of dependencies. However, many advanced features require additional packages. You can install these optional dependencies individually or in groups.

## Installation Syntax

To install optional dependencies, use the square bracket syntax with pip:

```bash
pip install "dremioframe[group_name]"
```

To install multiple groups:

```bash
pip install "dremioframe[group1,group2]"
```

## Dependency Groups

### Core Features

| Group | Dependencies | Features Enabled |
| :--- | :--- | :--- |
| `cli` | `rich`, `prompt_toolkit` | Enhanced CLI experience with rich text and interactive prompts for working with Orchestration and AI features. |
| `s3` | `boto3` | S3 integration for direct file operations and source management. |
| `scheduler` | `apscheduler` | Built-in task scheduling capabilities. |
| `dq` | `pyyaml` | Data Quality framework configuration parsing. |
| `ai` | `langchain`, `langchain-openai`, `langchain-anthropic`, `langchain-google-genai` | AI-powered Agent for Generating Python Scripts, SQL and cURL commands and light admin work. |
**note:** this libraries embdedded agent is primarily meant as a code generation assist tool, not meant as an alternative to the integrated Dremio agent for deeper administration and natural language analytics.

### File Formats & Export

| Group | Dependencies | Features Enabled |
| :--- | :--- | :--- |
| `excel` | `openpyxl` | Reading and writing Excel files. |
| `html` | `lxml`, `html5lib` | Parsing HTML tables. |
| `avro` | `fastavro` | Support for Avro file format. |
| `lance` | `pylance` | Support for Lance file format. |
| `image_export` | `kaleido` | Exporting Plotly charts as static images (PNG, JPG, PDF). |

### External Backends

| Group | Dependencies | Features Enabled |
| :--- | :--- | :--- |
| `postgres` | `psycopg2-binary` | Support for using PostgreSQL as an orchestration backend. |
| `mysql` | `mysql-connector-python` | Support for using MySQL as an orchestration backend. |
| `celery` | `celery`, `redis` | Distributed task execution using Celery and Redis. |

### Development & Documentation

| Group | Dependencies | Features Enabled |
| :--- | :--- | :--- |
| `dev` | `pytest`, `pytest-asyncio`, `requests-mock` | Running the test suite and contributing to DremioFrame. |
| `docs` | `mkdocs`, `mkdocs-material`, `mkdocstrings[python]` | Building and serving the documentation locally. |

## Feature-Specific Requirements

### Orchestration
- **Local Execution**: No extra dependencies required.
- **Distributed Execution**: Requires `celery`.
- **Persistent State**: Requires a backend like `postgres` or `mysql` (or uses local SQLite by default).

### AI Functions
To use functions like `ai_gen_sql` or `ai_analyze_data`, you must install the `ai` group:
```bash
pip install "dremioframe[ai]"
```

### Chart Exporting
To save charts as images using `chart.save("plot.png")`, you need the `image_export` group:
```bash
pip install "dremioframe[image_export]"
```
