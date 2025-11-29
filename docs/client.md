# Connecting to Dremio

`DremioClient` supports connecting to both Dremio Cloud and Dremio Software (Self-Managed).

## Dremio Cloud

By default, the client connects to Dremio Cloud.

```python
from dremioframe.client import DremioClient

# Uses DREMIO_PAT and DREMIO_PROJECT_ID from environment
client = DremioClient()

# Or explicitly
client = DremioClient(pat="my_pat", project_id="my_project_id")
```

## Dremio Software

To connect to a self-managed Dremio instance, provide the `hostname`, `port`, and credentials.

### Using Username/Password

```python
client = DremioClient(
    hostname="dremio.example.com",
    port=32010, # Default Flight port
    username="my_user",
    password="my_password",
    tls=True # Set to False if not using TLS
)
```

### Using PAT (Personal Access Token)

```python
client = DremioClient(
    hostname="dremio.example.com",
    port=32010,
    pat="my_pat",
    tls=True
)
```

### Disabling TLS Verification

If using self-signed certificates:

```python
client = DremioClient(
    hostname="localhost",
    port=32010,
    username="admin",
    password="password123",
    tls=True,
    disable_certificate_verification=True
)
```

## Environment Variables

You can also use environment variables, but currently `DremioClient` defaults to Cloud if `hostname` is not provided. To use Software via env vars, you still need to instantiate the client with at least the hostname.

Future versions may support `DREMIO_HOSTNAME` env var.
