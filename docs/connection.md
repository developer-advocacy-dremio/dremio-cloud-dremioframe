# Connecting to Dremio

This guide provides detailed instructions on how to connect `dremioframe` to your Dremio environment, whether it's Dremio Cloud or a self-managed Dremio Software instance.

## 1. Dremio Cloud

Dremio Cloud is the default connection mode. It uses Arrow Flight SQL over TLS.

### Prerequisites
- **Personal Access Token (PAT)**: Generate this in your Dremio Cloud User Settings.
- **Project ID**: The ID of the project you want to query.

### Connection Methods

#### Method A: Environment Variables (Recommended)
Set the following environment variables:
- `DREMIO_PAT`: Your Personal Access Token.
- `DREMIO_PROJECT_ID`: Your Project ID.

```python
from dremioframe.client import DremioClient

# Client automatically picks up env vars
client = DremioClient()
```

#### Method B: Explicit Parameters
Pass credentials directly to the constructor.

```python
client = DremioClient(
    pat="your_pat_here",
    project_id="your_project_id_here"
)
```

### Flight Configuration

You can specify a custom Flight endpoint and port if they differ from the main Dremio coordinator (e.g., when using a dedicated Flight executor).

```python
client = DremioClient(
    pat="my_token",
    flight_endpoint="flight.dremio.cloud",
    flight_port=443
)
```

---

## 2. Dremio Software (Self-Managed)

To connect to a Dremio Software cluster, you must specify the `hostname` and `port`.

### Prerequisites
- **Hostname**: The address of your Dremio coordinator (e.g., `dremio.example.com` or `localhost`).
- **Port**: The Arrow Flight port (default is `32010`).
- **Credentials**: Either Username/Password OR a Personal Access Token (PAT).

### Connection Methods

#### Method A: Username & Password
This uses Basic Authentication.

```python
client = DremioClient(
    hostname="localhost",
    port=32010,
    username="admin",
    password="password123",
    tls=False  # Set to True if your cluster has TLS enabled
)
```

#### Method B: Personal Access Token (PAT)
If you have generated a PAT for your user:

```python
client = DremioClient(
    hostname="localhost",
    port=32010,
    pat="your_pat_here",
    tls=False
)
```

### TLS/SSL Configuration

If your Dremio Software cluster uses TLS (Encryption), set `tls=True`.

#### Self-Signed Certificates
If you are using self-signed certificates (common in dev/test environments), you may need to disable certificate verification to avoid SSL errors.

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

> **Warning**: Disabling certificate verification is insecure and should not be used in production environments.

---

## 3. Troubleshooting & Common Errors

### `FlightUnavailableError` / Connection Refused
**Symptoms**: The client hangs or immediately raises an error saying the service is unavailable.
**Causes**:
- **Wrong Port**: Ensure you are using the **Arrow Flight Port** (default `32010`), NOT the UI port (`9047`) or the ODBC/JDBC port (`31010`).
- **Firewall**: Ensure the port is open and accessible from your machine.
- **Dremio Down**: Check if the Dremio service is running.

### `FlightUnauthenticatedError` / Auth Failed
**Symptoms**: "Invalid credentials" or "Unauthenticated".
**Causes**:
- **Expired PAT**: Tokens expire. Generate a new one.
- **Wrong Project ID**: For Dremio Cloud, ensure the Project ID matches the one associated with your token/user.
- **Typo**: Double-check username/password.

### `FlightInternalError` (Certificate Issues)
**Symptoms**: "Handshake failed", "Certificate verify failed".
**Causes**:
- **TLS Mismatch**: You set `tls=True` but the server is `tls=False` (or vice versa).
- **Self-Signed Cert**: You are connecting to a TLS-enabled server with a self-signed cert but haven't set `disable_certificate_verification=True` or provided the root CA.

### `ArrowInvalid` / Protocol Error
**Symptoms**: Errors parsing the response.
**Causes**:
- **HTTP vs Flight**: You might be trying to connect to the HTTP REST API port (`9047`) using the Arrow Flight client. Make sure `port` is set to the Flight port (`32010` for Software, `443` for Cloud).

## 4. Testing Connectivity

You can verify your connection by running a simple catalog list command:

```python
try:
    client = DremioClient(...)
    print("Connected!")
    print(client.catalog.list_catalog())
except Exception as e:
    print(f"Connection failed: {e}")
```
