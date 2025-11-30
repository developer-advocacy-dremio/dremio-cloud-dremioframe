# Contributing to DremioFrame

We welcome contributions! Here's how to get started.

## Development Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/developer-advocacy-dremio/dremio-cloud-dremioframe.git
    cd dremioframe
    ```

2.  **Install Dependencies**:
    We use `poetry` or `pip`.
    ```bash
    pip install -e ".[dev,all]"
    ```
    This installs all optional dependencies and dev tools (pytest, black, etc.).

## Running Tests

See [Testing Guide](docs/testing.md) for details.

- **Fast Tests**: `pytest -m "not software and not external_backend"`
- **All Tests**: `pytest`

## Code Style

We follow PEP 8. Please run formatting before submitting a PR:

```bash
black dremioframe tests
isort dremioframe tests
```

## Documentation

- Update docstrings for any new code.
- Update markdown files in `docs/` if you change functionality.
- We use MkDocs. Run `mkdocs serve` to preview changes.

## Pull Request Process

1.  Fork the repo and create a branch.
2.  Add tests for your changes.
3.  Ensure all tests pass.
4.  Submit a PR with a clear description of changes.
