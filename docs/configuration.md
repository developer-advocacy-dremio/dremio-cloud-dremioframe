# Configuration Reference

This guide details all configuration options for DremioFrame, including environment variables and client arguments.

## Environment Variables

### Dremio Cloud Connection
| Variable | Description | Required |
|----------|-------------|----------|
| `DREMIO_PAT` | Personal Access Token for authentication. | Yes (for Cloud) |
| `DREMIO_PROJECT_ID` | Project ID of the Dremio Cloud project. | Yes (for Cloud) |

### Dremio Software Connection
| Variable | Description | Default |
|----------|-------------|---------|
| `DREMIO_SOFTWARE_HOST` | Hostname of the Dremio coordinator. | `localhost` |
| `DREMIO_SOFTWARE_PORT` | Arrow Flight port. | `32010` |
| `DREMIO_SOFTWARE_USER` | Username for authentication. | - |
| `DREMIO_SOFTWARE_PASSWORD` | Password for authentication. | - |
| `DREMIO_SOFTWARE_TLS` | Enable TLS (`true`/`false`). | `false` |

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
        pat: str = None,          # Defaults to env DREMIO_PAT
        project_id: str = None,   # Defaults to env DREMIO_PROJECT_ID
        base_url: str = None,     # Defaults to https://api.dremio.cloud or http://localhost:9047/api/v3
        hostname: str = None,     # Flight hostname
        port: int = None,         # Flight port
        username: str = None,     # Software username
        password: str = None,     # Software password
        tls: bool = None,         # Enable TLS for Flight
        certs: str = None,        # Path to trusted certificates
        flight_endpoint: str = None # Custom Flight endpoint URL
    ):
        ...
```
