# Troubleshooting Guide

Common issues and solutions when using DremioFrame.

## Connectivity Issues

### Arrow Flight: Connection Refused
**Error**: `pyarrow.lib.ArrowIOError: Flight returned unavailable error, with message: failed to connect to all addresses`
**Cause**: The client cannot reach the Dremio Flight endpoint.
**Solution**:
- Verify `hostname` and `port` (default 32010 for Software, 443 for Cloud).
- Ensure firewall allows traffic on the Flight port.
- If using Docker, ensure ports are mapped (`-p 32010:32010`).

### Authentication Failed
**Error**: `pyarrow.flight.FlightUnauthenticatedError`
**Cause**: Invalid credentials.
**Solution**:
- **Cloud**: Check `DREMIO_PAT` is valid and not expired.
- **Software**: Verify username/password.
- Ensure `DREMIO_PROJECT_ID` is set for Cloud.

## Orchestration Issues

### Backend Import Errors
**Error**: `ImportError: psycopg2 is required...`
**Cause**: Missing optional dependencies.
**Solution**:
- Install with extras: `pip install "dremioframe[postgres]"` or `pip install "dremioframe[mysql]"`.

### Task Execution Fails Immediately
**Cause**: Missing environment variables or configuration in the execution environment (e.g., inside a Docker container).
**Solution**:
- Pass environment variables to the container or worker process.

## Debugging

Enable debug logging to see detailed request/response info:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This will output REST API calls and Flight connection details.
