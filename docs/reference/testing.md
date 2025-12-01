# Testing Guide

DremioFrame tests are categorized into three groups.

## 1. Unit & Integration (Dremio Cloud)
These tests cover the core logic and integration with Dremio Cloud. They should always pass.

**Requirements:**
- `DREMIO_PAT`: Personal Access Token for Dremio Cloud.
- `DREMIO_PROJECT_ID`: Project ID for Dremio Cloud.
- `DREMIO_TEST_SPACE`: Writable space/folder for integration tests (e.g., "Scratch").

**Command:**
```bash
# Run all unit tests and cloud integration tests
pytest -m "not software and not external_backend"
```
*Note: If credentials are missing, cloud integration tests will skip.*

## 2. Dremio Software
Tests specifically for Dremio Software connectivity.

**Requirements:**
- `DREMIO_SOFTWARE_HOST`: Hostname (e.g., localhost).
- `DREMIO_SOFTWARE_PORT`: Flight port (default 32010).
- `DREMIO_SOFTWARE_USER`: Username.
- `DREMIO_SOFTWARE_PASSWORD`: Password.
- `DREMIO_SOFTWARE_TLS`: "true" or "false" (default false).

**Command:**
```bash
pytest -m software
```

## 3. External Backends
Tests for persistent orchestration backends (Postgres, MySQL).

**Requirements:**
- **Postgres**: `DREMIOFRAME_PG_DSN` (e.g., `postgresql://user:pass@localhost/db`)
- **MySQL**:
    - `DREMIOFRAME_MYSQL_USER`
    - `DREMIOFRAME_MYSQL_PASSWORD`
    - `DREMIOFRAME_MYSQL_HOST`
    - `DREMIOFRAME_MYSQL_DB`
    - `DREMIOFRAME_MYSQL_PORT`

**Command:**
```bash
pytest -m external_backend
```

## Running All Tests
To run everything (skipping what isn't configured):
```bash
pytest
```
