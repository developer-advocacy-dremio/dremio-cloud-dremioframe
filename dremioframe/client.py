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

    def ingest_api(self, url: str, table_name: str, headers: dict = None, json_path: str = None, 
                   mode: str = 'append', pk: str = None, batch_size: int = None):
        """
        Ingest data from an API endpoint into Dremio.
        
        Args:
            url: The API URL.
            table_name: The target table name.
            headers: Optional headers for the request.
            json_path: Optional key to extract list of records from JSON response (e.g. "data.items").
            mode: 'replace', 'append', or 'merge'.
            pk: Primary key column for 'append' (incremental) or 'merge'.
            batch_size: Batch size for insertion.
        """
        import pandas as pd
        
        # 1. Fetch Data
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # 2. Parse Data
        if json_path:
            keys = json_path.split('.')
            for k in keys:
                data = data.get(k, [])
        
        if not isinstance(data, list):
            raise ValueError("API response (or extracted path) must be a list of records")
            
        df = pd.DataFrame(data)
        if df.empty:
            print("No data fetched from API.")
            return
            
        # 3. Handle Modes
        if mode == 'replace':
            # Drop table if exists
            try:
                # We need a drop table command.
                self.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass # Ignore if table doesn't exist
            # Create/Insert
            # Use create with data
            try:
                self.table(table_name).create(table_name, data=df, batch_size=batch_size)
            except Exception as e:
                # If create fails (e.g. table exists but delete failed?), try insert
                # But we tried to delete rows/drop table.
                raise e
            
        elif mode == 'append':
            if pk:
                # Get max PK
                try:
                    max_val_df = self.table(table_name).agg(m=f"MAX({pk})").collect()
                    max_val = max_val_df['m'][0]
                    if max_val is not None:
                        df = df[df[pk] > max_val]
                except Exception:
                    # Table might not exist or empty
                    pass
            
            if df.empty:
                print("No new records to append.")
                return
            
            # Insert
            # If table doesn't exist, insert might fail.
            # We should check existence or just try create if insert fails?
            # For now, assume table exists for append mode, or user should use replace first.
            # But if we want to be robust:
            try:
                self.table(table_name).insert(table_name, data=df, batch_size=batch_size)
            except Exception:
                # Try create if insert failed (maybe table doesn't exist)
                self.table(table_name).create(table_name, data=df, batch_size=batch_size)

        elif mode == 'merge':
            if not pk:
                raise ValueError("Merge mode requires a primary key (pk)")
            
            # 1. Create Staging Table
            staging_table = f"{table_name}_staging_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
            
            # Create staging table with data
            self.table(staging_table).create(staging_table, data=df, batch_size=batch_size)
            
            # 2. Merge
            # Check if target exists. If not, just rename staging to target?
            # Or just create target from staging.
            # Assuming target exists for merge.
            
            try:
                self.table(table_name).merge(
                    target_table=table_name,
                    on=pk,
                    matched_update={col: f"source.{col}" for col in df.columns if col != pk},
                    not_matched_insert={col: f"source.{col}" for col in df.columns},
                    data=None # We are using staging table as source
                )
                # Wait, merge takes `data` or uses `self` (builder).
                # We need to create a builder for the staging table.
                self.table(staging_table).merge(
                    target_table=table_name,
                    on=pk,
                    matched_update={col: f"source.{col}" for col in df.columns if col != pk},
                    not_matched_insert={col: f"source.{col}" for col in df.columns}
                )
            except Exception as e:
                # If target doesn't exist, maybe we should have just created it?
                # But merge implies existing data.
                # If target missing, we can just CTAS from staging.
                # Check if error is "Table not found"
                if "not found" in str(e).lower():
                     self.table(staging_table).create(table_name)
                else:
                    raise e
            finally:
                # 3. Drop Staging
                self.execute(f"DROP TABLE IF EXISTS {staging_table}")
