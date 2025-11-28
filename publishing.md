# Publishing DremioFrame to PyPI

This guide explains how to build and publish the `dremioframe` library to PyPI (Python Package Index).

## Prerequisites

1.  **PyPI Account**: You need an account on [PyPI](https://pypi.org/).
2.  **API Token**: Generate an API token in your PyPI account settings.
3.  **Build Tools**: Install `build` and `twine`.

```bash
pip install build twine
```

## Step 1: Update Version

Before publishing a new version, update the `version` in `pyproject.toml`:

```toml
[project]
version = "0.1.0"  # Increment this (e.g., 0.1.1)
```

## Step 2: Build the Package

Run the following command from the root directory to build the source distribution and wheel:

```bash
python -m build
```

This will create a `dist/` directory containing:
- `dremioframe-0.1.0.tar.gz` (Source)
- `dremioframe-0.1.0-py3-none-any.whl` (Wheel)

## Step 3: Check the Package

Run `twine check` to ensure the package description and metadata are correct:

```bash
twine check dist/*
```

## Step 4: Upload to PyPI

### TestPyPI (Recommended for first time)

First, upload to TestPyPI to verify everything looks good.

1.  Register on [TestPyPI](https://test.pypi.org/).
2.  Upload:

```bash
twine upload --repository testpypi dist/*
```

3.  Verify installation:

```bash
pip install --index-url https://test.pypi.org/simple/ --no-deps dremioframe
```

### Production PyPI

Once verified, upload to the real PyPI:

```bash
twine upload dist/*
```

You will be prompted for your username (`__token__`) and your API token (including the `pypi-` prefix).

## Automation (Optional)

You can automate this process using GitHub Actions. Create a workflow file `.github/workflows/publish.yml` to publish on release creation.
