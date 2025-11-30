# Documentation Enhancement Recommendations

To improve the usability and maintainability of DremioFrame's documentation, the following enhancements are recommended:

## 1. Centralized Configuration Reference
Currently, configuration options (environment variables, client arguments) are scattered across `README.md`, `testing.md`, and code.

**Recommendation**:
- Create `docs/configuration.md`.
- List all environment variables:
    - **Client**: `DREMIO_PAT`, `DREMIO_PROJECT_ID`, `DREMIO_SOFTWARE_HOST`, etc.
    - **Orchestration**: `DREMIOFRAME_PG_DSN`, `DREMIOFRAME_MYSQL_USER`, `CELERY_BROKER_URL`.
    - **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- Document `DremioClient` initialization parameters in detail.

## 2. End-to-End Tutorials
While feature-specific docs exist, a cohesive guide showing a real-world workflow is missing.

**Recommendation**:
- Create `docs/tutorial_etl.md`: "Building a Production ETL Pipeline".
    - Step 1: Connect to Dremio.
    - Step 2: Ingest data from an API to a raw table.
    - Step 3: Transform data using `DremioBuilder`.
    - Step 4: Validate data using the Data Quality Framework.
    - Step 5: Orchestrate the workflow with `Pipeline` and `Scheduler`.

## 3. Auto-Generated API Reference
Manual documentation of classes and methods is prone to becoming outdated.

**Recommendation**:
- Use **MkDocs** with `mkdocstrings` to generate API documentation directly from docstrings.
- Structure:
    - `reference/client.md`
    - `reference/builder.md`
    - `reference/orchestration.md`

## 4. Troubleshooting Guide
Users often face connectivity or configuration issues.

**Recommendation**:
- Create `docs/troubleshooting.md`.
- Common topics:
    - "Arrow Flight Connection Refused" (Port issues).
    - "Authentication Failed" (PAT vs Password).
    - "Missing Dependency" errors (optional dependencies).
    - How to enable debug logging (`logging.basicConfig(level=logging.DEBUG)`).

## 5. Architecture Diagrams
Visual aids help users understand the system design.

**Recommendation**:
- Add Mermaid diagrams to `architecture.md`.
- **System Overview**: Client <-> REST API / Flight <-> Dremio.
- **Orchestration Flow**: Scheduler -> Pipeline -> Task -> Executor -> Backend.

## 6. Contributing Guide
To encourage community contributions.

**Recommendation**:
- Create `CONTRIBUTING.md`.
- Setup instructions (poetry/pip).
- Testing guidelines (referencing `docs/testing.md`).
- Code style (Black, Isort).
