import os
import requests
from .catalog import Catalog
from .builder import DremioBuilder
from .utils import get_env_var

class DremioClient:
    def __init__(self, pat: str = None, project_id: str = None, base_url: str = "https://api.dremio.cloud/v0"):
        self.pat = pat or os.getenv("DREMIO_PAT")
        self.project_id = project_id or os.getenv("DREMIO_PROJECT_ID")
        self.base_url = base_url

        if not self.pat:
            raise ValueError("Dremio PAT is required.")
        
        # If project_id is missing, we might want to try to fetch it, but for now we'll just warn or error if needed.
        # However, for the catalog API, project_id is usually required in the URL.
        
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.pat}",
            "Content-Type": "application/json"
        })

        self.catalog = Catalog(self)

    def table(self, path: str) -> DremioBuilder:
        return DremioBuilder(self, path)

    def sql(self, query: str):
        # This will return a builder initialized with a SQL query or directly execute it.
        # For now, let's return a builder that can execute raw SQL.
        return DremioBuilder(self, sql=query)

    def execute(self, query: str):
        """Execute raw SQL query directly via Flight"""
        # Create a temporary builder just to access _execute_flight
        # Or better, move _execute_flight to client or utils?
        # For now, just use a builder
        return DremioBuilder(self)._execute_flight(query, "polars")

    def external_query(self, source: str, sql: str) -> 'DremioBuilder':
        """
        Create a builder from an external query.
        
        Args:
            source: The name of the source (e.g., "Postgres").
            sql: The native SQL query to run on the source.
        """
        # Escape single quotes in the SQL string
        escaped_sql = sql.replace("'", "''")
        query = f"SELECT * FROM TABLE({source}.EXTERNAL_QUERY('{escaped_sql}'))"
        return DremioBuilder(self, sql=query)
