# Deployment Guide

DremioFrame Orchestration is designed to be easily deployed using Docker.

## Docker Deployment

We provide a `Dockerfile` and `docker-compose.yml` to get you started quickly with a full stack including:
- **Orchestrator**: Runs the Web UI and Scheduler.
- **Worker**: Runs Celery workers for distributed tasks.
- **Postgres**: Persistent backend for pipeline history.
- **Redis**: Message broker for Celery.

### Prerequisites
- Docker and Docker Compose installed.

### Quick Start

1. **Configure Environment**:
   Create a `.env` file with your Dremio credentials:
   ```bash
   DREMIO_PAT=your_pat
   DREMIO_PROJECT_ID=your_project_id
   ```

2. **Start Services**:
   ```bash
   docker-compose up -d
   ```

3. **Access UI**:
   Open `http://localhost:8080` in your browser.

### Customizing the Image

If you need additional Python packages (e.g. for custom tasks), you can extend the Dockerfile:

```dockerfile
FROM dremioframe:latest
RUN pip install pandas numpy
```

### Production Considerations

- **Security**: Enable Basic Auth by setting `USERNAME` and `PASSWORD` env vars (requires updating entrypoint script) or putting the UI behind a reverse proxy (Nginx/Traefik).
- **Database**: Use a managed Postgres instance (RDS/CloudSQL) instead of the containerized one for production data safety.
- **Scaling**: Scale workers using Docker Compose:
  ```bash
  docker-compose up -d --scale worker=3
  ```
