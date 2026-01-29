# Connecting to Dremio

This guide provides detailed instructions on how to connect `dremioframe` to your Dremio environment, whether it's Dremio Cloud or a self-managed Dremio Software instance.

## Overview

DremioFrame supports three connection modes:
- **`cloud`**: Dremio Cloud (SaaS)
- **`v26`**: Dremio Software v26+ (with PAT support)
- **`v25`**: Dremio Software v25 and earlier

The `mode` parameter automatically configures ports, endpoints, and authentication methods. In most cases, the mode is auto-detected, but you can specify it explicitly for clarity.

---

## 1. Connection Profiles (Recommended)

Managing multiple Dremio environments (e.g., dev, prod, cloud, software) is easiest using a credentials file.

### Location
Create a file at `~/.dremio/profiles.yaml`.

### Sample Configuration
```yaml
profiles:
  # Dremio Cloud Example
  cloud_dev:
    type: cloud
    auth:
      type: pat
      token: "your-cloud-pat"
    project_id: "your-project-id"

  # Dremio Software v26+ Example (PAT)
  software_prod:
    type: software
    base_url: "https://dremio.company.com"
    auth:
      type: pat
      token: "your-software-pat"
    ssl: "true"

  # Dremio Software v25 Example (User/Pass)
  software_legacy:
    type: software
    base_url: "http://dremio-old.company.com:9047"
    auth:
      type: username_password
      username: "admin"
      password: "password123"
    ssl: "false"

default_profile: cloud_dev
```

### Usage

```python
from dremioframe.client import DremioClient

# Uses the 'default_profile' (cloud_dev)
client = DremioClient()

# Uses a specific profile
client_prod = DremioClient(profile="software_prod")
```

> **Note**: You can generate this file with the `dremio-cli` python library or create it manually.
---

## 2. Dremio Cloud

Dremio Cloud is the default connection mode. It uses Arrow Flight SQL over TLS.

### Prerequisites
- **Personal Access Token (PAT)**: Generate this in your Dremio Cloud User Settings
- **Project ID**: The ID of the project you want to query

### Environment Variables

Set these in your `.env` file or environment:

```bash
DREMIO_PAT=your_cloud_personal_access_token
DREMIO_PROJECT_ID=your_project_id_here
```

### Connection Examples

#### Using Environment Variables (Recommended)

```python
from dremioframe.client import DremioClient

# Client automatically picks up env vars and detects Cloud mode
client = DremioClient()

# Or explicitly specify mode for clarity
client = DremioClient(mode="cloud")
```

#### Using Explicit Parameters

```python
client = DremioClient(
    pat="your_pat_here",
    project_id="your_project_id_here",
    mode="cloud"  # Optional, auto-detected when project_id is provided
)
```

### Custom Flight Configuration

You can specify a custom Flight endpoint if needed:

```python
client = DremioClient(
    pat="my_token",
    project_id="my_project",
    flight_endpoint="flight.dremio.cloud",
    flight_port=443,
    mode="cloud"
)
```

### Default Ports (Cloud)
- **REST API**: Port 443 (`https://api.dremio.cloud/v0`)
- **Arrow Flight**: Port 443 (`grpc+tls://data.dremio.cloud:443`)

### Service User Login (OAuth Client Credentials)

For automation scripts (CI/CD), you can authenticate as a Service User using a Client ID and Secret instead of a PAT.

**Prerequisites:**
1. Create a Service User via `client.admin.create_user(..., identity_type="SERVICE_USER")`.
2. Generate a Client Secret via `client.admin.credentials.create(...)`.

**Environment Variables:**
```bash
DREMIO_CLIENT_ID=your_service_user_uuid
DREMIO_CLIENT_SECRET=your_client_secret
DREMIO_PROJECT_ID=your_project_id
```

**Usage:**
```python
client = DremioClient(mode="cloud")
# Automatically uses DREMIO_CLIENT_ID/SECRET if DREMIO_PAT is not set.
```

Or explicit:
```python
client = DremioClient(
    mode="cloud",
    client_id="user_uuid",
    client_secret="secret_value",
    project_id="project_id"
)
```

---

## 3. Dremio Software v26+

Dremio Software v26+ supports Personal Access Tokens (PAT) for authentication, similar to Cloud.

### Prerequisites
- **Hostname**: The address of your Dremio coordinator (e.g., `v26.dremio.org` or `localhost`)
- **Personal Access Token (PAT)** OR **Username/Password**
- **TLS**: Whether your instance uses TLS/SSL

### Environment Variables

Set these in your `.env` file:

```bash
DREMIO_SOFTWARE_HOST=https://v26.dremio.org
DREMIO_SOFTWARE_PAT=your_software_personal_access_token
DREMIO_SOFTWARE_TLS=true

# Optional: Override default ports
# DREMIO_SOFTWARE_PORT=443
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

**Note**: `DREMIO_SOFTWARE_HOST` can include the protocol (`https://` or `http://`). If TLS is true and no port is specified in the URL, port 443 is used for REST API.

> **Important**: For Arrow Flight connections in v26+, the client uses Basic Authentication (Username + PAT). If you only provide a PAT, the client will attempt to automatically discover your username from the catalog. If discovery fails, you may need to provide `DREMIO_SOFTWARE_USER` explicitly.

### Connection Examples

#### Using PAT with Environment Variables (Recommended)

```python
from dremioframe.client import DremioClient

# Auto-detects v26 mode from DREMIO_SOFTWARE_* env vars
client = DremioClient()

# Or explicitly specify mode
client = DremioClient(mode="v26")
```

#### Using PAT with Explicit Parameters

```python
client = DremioClient(
    hostname="v26.dremio.org",
    pat="your_pat_here",
    tls=True,
    mode="v26"
)
```

#### Using Username/Password

```python
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password123",
    tls=False,
    mode="v26"
)
```

### Default Ports (v26)
- **REST API**: Port 443 (with TLS) or 9047 (without TLS)
- **Arrow Flight**: Port 32010
- **Login Endpoint**: `/api/v3/login` or `/apiv3/login`

### TLS/SSL Configuration

If your Dremio Software cluster uses TLS (Encryption), set `tls=True`:

```python
client = DremioClient(
    hostname="secure.dremio.com",
    pat="your_pat",
    tls=True,
    mode="v26"
)
```

#### Self-Signed Certificates

For self-signed certificates (common in dev/test environments):

```python
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password123",
    tls=True,
    disable_certificate_verification=True,
    mode="v26"
)
```

> **Warning**: Disabling certificate verification is insecure and should not be used in production.

---

## 4. Dremio Software v25 and Earlier

Dremio Software v25 and earlier versions use username/password authentication only (no PAT support).

### Prerequisites
- **Hostname**: The address of your Dremio coordinator
- **Username and Password**: Valid Dremio credentials

### Environment Variables

Set these in your `.env` file:

```bash
DREMIO_SOFTWARE_HOST=localhost
DREMIO_SOFTWARE_USER=admin
DREMIO_SOFTWARE_PASSWORD=password123
DREMIO_SOFTWARE_TLS=false

# Optional: Override default ports
# DREMIO_SOFTWARE_PORT=9047
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

### Connection Examples

#### Using Environment Variables

```python
from dremioframe.client import DremioClient

client = DremioClient(mode="v25")
```

#### Using Explicit Parameters

```python
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password123",
    tls=False,
    mode="v25"
)
```

### Default Ports (v25)
- **REST API**: Port 9047 (`http://localhost:9047/api/v3`)
- **Arrow Flight**: Port 32010
- **Login Endpoint**: `/apiv2/login`

---

## 4. Port Configuration Reference

Ports are automatically configured based on the `mode`, but can be overridden:

| Mode | REST Port (default) | Flight Port (default) | Protocol |
|------|---------------------|----------------------|----------|
| `cloud` | 443 | 443 | HTTPS/gRPC+TLS |
| `v26` (TLS) | 443 | 32010 | HTTPS/gRPC+TLS |
| `v26` (no TLS) | 9047 | 32010 | HTTP/gRPC+TCP |
| `v25` | 9047 | 32010 | HTTP/gRPC+TCP |

### Overriding Ports

```python
# Custom REST API port
client = DremioClient(
    hostname="custom.dremio.com",
    port=8443,
    flight_port=31010,
    mode="v26"
)
```

---

## 5. Mode Auto-Detection

The client automatically detects the mode based on:

1. **Cloud mode** is detected when:
   - `hostname == "data.dremio.cloud"`, OR
   - `project_id` is provided, OR
   - `DREMIO_PROJECT_ID` environment variable is set

2. **v26 mode** is detected when:
   - `DREMIO_SOFTWARE_HOST` or `DREMIO_SOFTWARE_PAT` environment variables are set

3. **Default**: Falls back to `cloud` mode if no indicators are found

### Explicit Mode Selection

For clarity and to avoid ambiguity, you can always specify the mode explicitly:

```python
# Force v26 mode
client = DremioClient(
    hostname="my-dremio.com",
    pat="...",
    mode="v26"
)

# Force cloud mode
client = DremioClient(
    pat="...",
    project_id="...",
    mode="cloud"
)

# Force v25 mode
client = DremioClient(
    hostname="localhost",
    username="admin",
    password="password",
    mode="v25"
)
```

---

## 6. Complete .env File Examples

### For Dremio Cloud

```bash
# Required
DREMIO_PAT=dremio_pat_abc123xyz456...
DREMIO_PROJECT_ID=12345678-1234-1234-1234-123456789abc

# Optional
# DREMIO_FLIGHT_ENDPOINT=data.dremio.cloud
# DREMIO_FLIGHT_PORT=443
```

### For Dremio Software v26+ (with PAT)

```bash
# Required
DREMIO_SOFTWARE_HOST=https://v26.dremio.org
DREMIO_SOFTWARE_PAT=dremio_pat_xyz789...
DREMIO_SOFTWARE_TLS=true

# Optional
# DREMIO_SOFTWARE_PORT=443
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

### For Dremio Software v26+ (with Username/Password)

```bash
# Required
DREMIO_SOFTWARE_HOST=localhost
DREMIO_SOFTWARE_USER=admin
DREMIO_SOFTWARE_PASSWORD=password123
DREMIO_SOFTWARE_TLS=false

# Optional
# DREMIO_SOFTWARE_PORT=9047
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

### For Dremio Software v25

```bash
# Required
DREMIO_SOFTWARE_HOST=localhost
DREMIO_SOFTWARE_USER=admin
DREMIO_SOFTWARE_PASSWORD=password123
DREMIO_SOFTWARE_TLS=false

# Optional
# DREMIO_SOFTWARE_PORT=9047
# DREMIO_SOFTWARE_FLIGHT_PORT=32010
```

---

## 7. Troubleshooting & Common Errors

### `FlightUnavailableError` / Connection Refused

**Symptoms**: The client hangs or raises an error saying the service is unavailable.

**Causes**:
- **Wrong Port**: Ensure you are using the **Arrow Flight Port** (default `32010` for Software, `443` for Cloud), NOT the UI port (`9047`) or ODBC/JDBC port (`31010`)
- **Firewall**: Ensure the port is open and accessible
- **Dremio Down**: Check if the Dremio service is running
- **Wrong Mode**: Ensure you're using the correct mode (`cloud`, `v26`, or `v25`)

**Solution**:
```python
# Verify your mode and ports
client = DremioClient(
    hostname="your-host",
    mode="v26",  # Explicitly set mode
    flight_port=32010  # Explicitly set Flight port if needed
)
```

### `FlightUnauthenticatedError` / Auth Failed

**Symptoms**: "Invalid credentials" or "Unauthenticated".

**Causes**:
- **Expired PAT**: Tokens expire. Generate a new one
- **Wrong Project ID**: For Cloud, ensure the Project ID matches
- **Wrong Mode**: Using Cloud credentials with Software mode or vice versa
- **Typo**: Double-check credentials

**Solution**:
```python
# For Cloud, ensure both PAT and project_id are correct
client = DremioClient(pat="...", project_id="...", mode="cloud")

# For Software v26+, ensure PAT is valid
client = DremioClient(hostname="...", pat="...", mode="v26")

# For Software v25, use username/password
client = DremioClient(hostname="...", username="...", password="...", mode="v25")
```

### `FlightInternalError` (Certificate Issues)

**Symptoms**: "Handshake failed", "Certificate verify failed".

**Causes**:
- **TLS Mismatch**: You set `tls=True` but the server uses `tls=False` (or vice versa)
- **Self-Signed Cert**: Connecting to TLS-enabled server with self-signed certificate

**Solution**:
```python
# For self-signed certificates
client = DremioClient(
    hostname="localhost",
    tls=True,
    disable_certificate_verification=True,
    mode="v26"
)
```

### Environment Variable Conflicts

**Symptoms**: Client connects to wrong environment or uses wrong credentials.

**Causes**:
- Both `DREMIO_PAT` and `DREMIO_SOFTWARE_PAT` are set
- Both Cloud and Software environment variables are set

**Solution**:
```python
# Explicitly specify mode to avoid ambiguity
client = DremioClient(mode="v26")  # Forces Software mode

# Or unset conflicting environment variables
import os
if "DREMIO_PROJECT_ID" in os.environ:
    del os.environ["DREMIO_PROJECT_ID"]
```

---

## 8. Testing Connectivity

Verify your connection with a simple test:

```python
from dremioframe.client import DremioClient

try:
    # Create client (adjust mode as needed)
    client = DremioClient(mode="v26")
    
    # Test catalog access
    catalog = client.catalog.list_catalog()
    print(f"✅ Connected successfully! Found {len(catalog)} catalog items.")
    
    # Test query execution (requires Arrow Flight)
    result = client.query("SELECT 1 as test")
    print(f"✅ Query execution successful!")
    print(result)
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    import traceback
    traceback.print_exc()
```

### Quick Diagnostic Script

```python
from dremioframe.client import DremioClient
import os

print("Environment Variables:")
print(f"  DREMIO_PAT: {'SET' if os.getenv('DREMIO_PAT') else 'NOT SET'}")
print(f"  DREMIO_PROJECT_ID: {'SET' if os.getenv('DREMIO_PROJECT_ID') else 'NOT SET'}")
print(f"  DREMIO_SOFTWARE_HOST: {os.getenv('DREMIO_SOFTWARE_HOST', 'NOT SET')}")
print(f"  DREMIO_SOFTWARE_PAT: {'SET' if os.getenv('DREMIO_SOFTWARE_PAT') else 'NOT SET'}")
print(f"  DREMIO_SOFTWARE_USER: {os.getenv('DREMIO_SOFTWARE_USER', 'NOT SET')}")

client = DremioClient(mode="v26")  # Adjust mode as needed
print(f"\nClient Configuration:")
print(f"  Mode: {client.mode}")
print(f"  Hostname: {client.hostname}")
print(f"  REST Port: {client.port}")
print(f"  Flight Port: {client.flight_port}")
print(f"  Base URL: {client.base_url}")
print(f"  Project ID: {client.project_id}")
```
