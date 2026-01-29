# Configuration Reference

This guide details all configuration options for DremioFrame, including environment variables and client arguments.

## Quick Start Examples

### Dremio Cloud
```python
from dremioframe.client import DremioClient

# Simplest - uses environment variables DREMIO_PAT and DREMIO_PROJECT_ID
client = DremioClient()

# Or specify explicitly
client = DremioClient(
    pat="your_pat_here",
    project_id="your_project_id",
    mode="cloud"  # Optional, auto-detected
)
```

### Dremio Software v26+
```python
# With PAT (recommended for v26+)
client = DremioClient(
    hostname="v26.dremio.org",
    pat="your_pat_here",
    tls=True,
    mode="v26"  # Automatically sets correct ports
)

# With username/password
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password123",
    tls=False,
    mode="v26"
)
```

### Dremio Software v25
```python
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password123",
    mode="v25"  # Uses v25-specific endpoints
)
```

## Connection Profiles (Recommended)

You can manage credentials using `~/.dremio/profiles.yaml`. This avoids hardcoding secrets in scripts.

### Sample `profiles.yaml`

```yaml
profiles:
  dev:
    type: cloud
    auth:
      type: pat
      token: "your-cloud-pat"
    project_id: "your-project-id"
  prod:
    type: software
    base_url: "https://dremio.prod.corp"
    auth:
      type: username_password
      username: "admin"
      password: "password123"
default_profile: dev
```

> **Note**: You can generate this file with the `dremio-cli` python library or create it manually.

## Environment Variables

### Dremio Cloud Connection
| Variable | Description | Required |
|----------|-------------|----------|
| `DREMIO_PAT` | Personal Access Token for authentication. | Yes (for Cloud) |
| `DREMIO_PROJECT_ID` | Project ID of the Dremio Cloud project. | Yes (for Cloud) |

### Dremio Software Connection
| Variable | Description | Default |
|----------|-------------|---------|
| `DREMIO_SOFTWARE_HOST` | Hostname or URL of the Dremio coordinator. | `localhost` |
| `DREMIO_SOFTWARE_PAT` | Personal Access Token (v26+ only). | - |
| `DREMIO_SOFTWARE_USER` | Username (optional for v26+ with PAT, required for v25). | - |
| `DREMIO_SOFTWARE_PORT` | REST API port (optional, auto-detected). | `443` (TLS) or `9047` |
| `DREMIO_SOFTWARE_FLIGHT_PORT` | Arrow Flight port (optional, auto-detected). | `32010` |
| `DREMIO_SOFTWARE_PASSWORD` | Password for authentication. | - |
| `DREMIO_SOFTWARE_TLS` | Enable TLS (`true`/`false`). | `false` |
| `DREMIO_ICEBERG_URI` | Iceberg Catalog REST URI (Required for Software). | `https://catalog.dremio.cloud/api/iceberg` (Cloud default) |

### Orchestration Backend
| Variable | Description | Example |
|----------|-------------|---------|
| `DREMIOFRAME_PG_DSN` | PostgreSQL connection string. | `postgresql://user:pass@host/db` |
| `DREMIOFRAME_MYSQL_USER` | MySQL username. | `root` |
| `DREMIOFRAME_MYSQL_PASSWORD` | MySQL password. | `password` |
| `DREMIOFRAME_MYSQL_HOST` | MySQL host. | `localhost` |
| `DREMIOFRAME_MYSQL_DB` | MySQL database name. | `dremioframe` |
| `DREMIOFRAME_MYSQL_PORT` | MySQL port. | `3306` |

### Celery Executor
| Variable | Description | Default |
|----------|-------------|---------|
| `CELERY_BROKER_URL` | Broker URL for Celery (Redis/RabbitMQ). | `redis://localhost:6379/0` |
| `CELERY_RESULT_BACKEND` | Backend for Celery results. | `redis://localhost:6379/0` |

### AWS / S3 Task
| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS Access Key. |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Key. |
| `AWS_DEFAULT_REGION` | AWS Region (e.g., `us-east-1`). |

## DremioClient Arguments

When initializing `DremioClient`, you can pass arguments directly or rely on environment variables.

```python
class DremioClient:
    def __init__(
        self,
        # Authentication
        pat: str = None,                    # Personal Access Token (Cloud or Software v26+)
        username: str = None,               # Username (Software with user/pass auth)
        password: str = None,               # Password (Software with user/pass auth)
        project_id: str = None,             # Project ID (Cloud only)
        
        # Connection Mode
        mode: str = None,                   # 'cloud', 'v26', or 'v25' (auto-detected if None)
        
        # Endpoints
        hostname: str = "data.dremio.cloud", # Dremio hostname
        port: int = None,                   # REST API port (auto-detected based on mode)
        base_url: str = None,               # Custom base URL (overrides auto-detection)
        
        # Arrow Flight
        flight_port: int = None,            # Arrow Flight port (auto-detected based on mode)
        flight_endpoint: str = None,        # Arrow Flight endpoint (defaults to hostname)
        
        # Security
        tls: bool = True,                   # Enable TLS/SSL
        disable_certificate_verification: bool = False  # Disable SSL cert verification
    ):
        ...
```

## Connection Mode Details

The `mode` parameter automatically configures ports and endpoints for different Dremio versions:

### `mode="cloud"` (Default)
- **REST API**: `https://api.dremio.cloud/v0` (port 443)
- **Arrow Flight**: `grpc+tls://data.dremio.cloud:443`
- **Authentication**: PAT with Bearer token
- **Auto-detected when**: `hostname == "data.dremio.cloud"` or `project_id` is set

### `mode="v26"` (Dremio Software v26+)
- **REST API**: `https://{hostname}:{port}/api/v3` (port 443 with TLS, 9047 without)
- **Arrow Flight**: `grpc+tls://{hostname}:32010` (or `grpc+tcp` without TLS)
- **Authentication**: PAT with Bearer token or username/password
- **Auto-detected when**: `DREMIO_SOFTWARE_HOST` or `DREMIO_SOFTWARE_PAT` env vars are set

### `mode="v25"` (Dremio Software v25 and earlier)
- **REST API**: `http://{hostname}:9047/api/v3`
- **Arrow Flight**: `grpc+tcp://{hostname}:32010`
- **Authentication**: Username/password only
- **Login Endpoint**: `/apiv2/login`

## Port Configuration

Ports are automatically configured based on the `mode`, but can be overridden:

| Mode | REST Port (default) | Flight Port (default) |
|------|---------------------|----------------------|
| `cloud` | 443 | 443 |
| `v26` (TLS) | 443 | 32010 |
| `v26` (no TLS) | 9047 | 32010 |
| `v25` | 9047 | 32010 |

**Override examples:**
```python
# Custom REST API port
client = DremioClient(hostname="custom.dremio.com", port=8443, mode="v26")

# Custom Flight port
client = DremioClient(hostname="custom.dremio.com", flight_port=31010, mode="v26")
```

## Example .env File

### For Dremio Cloud
```bash
DREMIO_PAT=your_cloud_pat_here
DREMIO_PROJECT_ID=your_project_id_here
```

### For Dremio Software v26+
```bash
DREMIO_SOFTWARE_HOST=https://v26.dremio.org
DREMIO_SOFTWARE_PAT=your_software_pat_here
DREMIO_SOFTWARE_TLS=true
# Optional: Override default ports
# DREMIO_SOFTWARE_PORT=443
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

### For Dremio Software v25
```bash
DREMIO_SOFTWARE_HOST=localhost
DREMIO_SOFTWARE_USER=admin
DREMIO_SOFTWARE_PASSWORD=password123
DREMIO_SOFTWARE_TLS=false
```

## Troubleshooting

### Connection Issues

**Problem**: "Connection timeout" or "Connection refused"
- **Solution**: Verify the hostname and ports are correct. For Software, ensure the Dremio coordinator is running.

**Problem**: "Authentication failed"
- **Solution**: 
  - For Cloud: Verify your PAT and Project ID are correct
  - For Software v26+: Ensure your PAT has the necessary permissions
  - For Software v25: Verify username/password are correct

**Problem**: "Flight queries fail but REST API works"
- **Solution**: Check that the `flight_port` is correct (default: 32010 for Software). Verify Arrow Flight is enabled on your Dremio instance.

### Mode Selection

If auto-detection isn't working correctly, explicitly set the `mode` parameter:

```python
# Force v26 mode
client = DremioClient(hostname="my-dremio.com", pat="...", mode="v26")

# Force cloud mode
client = DremioClient(pat="...", project_id="...", mode="cloud")
```
