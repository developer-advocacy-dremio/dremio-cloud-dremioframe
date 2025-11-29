import os
import requests
from .catalog import Catalog
from .builder import DremioBuilder
from .utils import get_env_var

class DremioClient:
    def __init__(self, pat: str = None, project_id: str = None, base_url: str = None,
                 hostname: str = "data.dremio.cloud", port: int = 443,
                 username: str = None, password: str = None, tls: bool = True,
                 disable_certificate_verification: bool = False):
        
        self.pat = pat or os.getenv("DREMIO_PAT")
        self.project_id = project_id or os.getenv("DREMIO_PROJECT_ID")
        
        # Connection details
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.tls = tls
        self.disable_certificate_verification = disable_certificate_verification
        
        # Determine Base URL for Catalog API
        if base_url:
            self.base_url = base_url
        elif hostname == "data.dremio.cloud":
            self.base_url = "https://api.dremio.cloud/v0"
        else:
            # Default Software REST API
            protocol = "https" if tls else "http"
            self.base_url = f"{protocol}://{hostname}:9047/api/v3"

        # Validation
        if not self.pat and not (self.username and self.password):
            raise ValueError("Either PAT or Username/Password is required.")
        
        self.session = requests.Session()
        if self.pat:
            self.session.headers.update({"Authorization": f"Bearer {self.pat}"})
        elif self.username and self.password:
            # For REST API, we might need to login to get a token.
            # Dremio Software /apiv3/login
            try:
                login_url = f"{self.base_url}/login"
                # Only attempt if we think it's a valid REST endpoint
                # This might fail if the user only wants Flight and didn't configure REST port
                # But Catalog requires REST.
                pass 
            except Exception:
                pass
            # For now, we'll assume Flight is the primary goal. 
            # Catalog might not work with User/Pass without implementing the login flow.
            
        self.session.headers.update({
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
