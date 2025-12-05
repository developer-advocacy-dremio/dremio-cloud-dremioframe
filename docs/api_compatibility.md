# Dremio API Compatibility Guide

`dremioframe` is designed to work seamlessly with Dremio Cloud, Dremio Software v26+, and Dremio Software v25. This document outlines how the library handles the differences between these versions.

## Supported Modes

The `DremioClient` accepts a `mode` parameter to configure behavior:

| Mode | Description | Default Auth | Base URL Pattern |
|------|-------------|--------------|------------------|
| `cloud` | Dremio Cloud | PAT + Project ID | `https://api.dremio.cloud/v0` |
| `v26` | Software v26+ | PAT (or User/Pass) | `https://{host}:9047/api/v3` |
| `v25` | Software v25 | User/Pass (or PAT) | `https://{host}:9047/api/v3` |

## Key Differences & Handling

### 1. Authentication

- **Cloud**: Uses Personal Access Tokens (PAT) exclusively. Requires `DREMIO_PAT` and `DREMIO_PROJECT_ID`.
- **Software v26**: Supports PATs natively. Can also use Username/Password to obtain a token via `/apiv3/login`.
- **Software v25**: Primarily uses Username/Password via `/apiv2/login` (legacy) or `/login` to obtain a token.

`dremioframe` automatically selects the correct login endpoint and token handling based on the `mode`.

### 2. API Endpoints

- **Project ID**: Dremio Cloud API paths typically require a project ID (e.g., `/v0/projects/{id}/catalog`). Dremio Software paths do not (e.g., `/api/v3/catalog`).
- **Base URL**: Cloud uses `/v0`, Software uses `/api/v3`.

The library's internal `_build_url` methods in `Catalog` and `Admin` classes automatically append the project ID for Cloud mode and omit it for Software mode.

### 3. Feature Support

| Feature | Cloud | Software | Handling |
|---------|-------|----------|----------|
| **Create Space** | No (Use Folders) | Yes | `create_space` raises `NotImplementedError` in Cloud mode. |
| **Reflections** | Yes | Yes | Unified interface via `admin.list_reflections`, etc. |
| **SQL Runner** | Yes | Yes | Unified via `client.query()`. |
| **Flight** | `data.dremio.cloud` | `{host}:32010` | Port and endpoint auto-configured. |

## Best Practices

- **Use Environment Variables**: Set `DREMIO_PAT`, `DREMIO_PROJECT_ID` (for Cloud), or `DREMIO_SOFTWARE_PAT`, `DREMIO_SOFTWARE_HOST` (for Software) to switch environments easily without changing code.
- **Check Mode**: If writing scripts that run across environments, check `client.mode` before calling software-specific methods like `create_space`.

```python
if client.mode == 'cloud':
    client.admin.create_folder("my_folder")
else:
    client.admin.create_space("my_space")
```
