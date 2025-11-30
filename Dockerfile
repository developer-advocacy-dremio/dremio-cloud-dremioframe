FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (e.g. for postgres/mysql drivers if needed)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install dremioframe with all orchestration extras
RUN pip install --no-cache-dir ".[postgres,mysql,celery,s3,scheduler]"

# Default command (can be overridden)
CMD ["dremio-cli", "pipeline", "ui", "--port", "8080"]
