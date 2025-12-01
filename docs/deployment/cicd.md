# CI/CD & Deployment

Deploying `dremioframe` pipelines and managing Dremio resources (Views, Reflections, RBAC) should be automated using CI/CD.

## 1. Managing Resources as Code

Store your Dremio logic in a version-controlled repository.

*   **Views**: Define views as SQL files or Python scripts using `create_view`.
*   **Reflections**: Define reflection configurations in JSON/YAML or Python scripts.

### Example: Deploy Script (`deploy.py`)

```python
import os
from dremioframe.client import DremioClient

client = DremioClient()

def deploy_view(view_name, sql_file):
    with open(sql_file, "r") as f:
        sql = f.read()
    client.catalog.create_view(["Space", view_name], sql, overwrite=True)
    print(f"Deployed {view_name}")

if __name__ == "__main__":
    deploy_view("sales_summary", "views/sales_summary.sql")
```

## 2. GitHub Actions Workflow

Here is an example workflow to deploy changes when merging to `main`.

```yaml
name: Deploy to Dremio

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      DREMIO_PAT: ${{ secrets.DREMIO_PAT }}
      DREMIO_PROJECT_ID: ${{ secrets.DREMIO_PROJECT_ID }}
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: pip install dremioframe
        
      - name: Run Deploy Script
        run: python deploy.py
```

## 3. Environment Management

Use different Spaces or prefixes for environments (Dev, Staging, Prod).

```python
# deploy.py
env = os.environ.get("ENV", "dev")
space = f"Marketing_{env}"

client.catalog.create_view([space, "view_name"], ...)
```

## 4. Testing

Run Data Quality checks as part of your CI pipeline before deploying.

```bash
# In CI step
dremio-cli dq run tests/dq
```
