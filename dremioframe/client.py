import os
import requests
from .catalog import Catalog
from .builder import DremioBuilder
from .admin import Admin
from .udf import UDFManager
from .utils import get_env_var

class DremioClient:
    def __init__(self, pat: str = None, project_id: str = None, base_url: str = None,
                 hostname: str = "data.dremio.cloud", port: int = None,
                 username: str = None, password: str = None, tls: bool = True,
                 disable_certificate_verification: bool = False,
                 flight_port: int = None, flight_endpoint: str = None,
                 mode: str = "cloud"):
        """
        Initialize Dremio Client.
        
        Args:
            pat: Personal Access Token (for Cloud or Software v26+)
            project_id: Project ID (Cloud only, set to None for Software)
            base_url: Custom base URL (auto-detected if not provided)
            hostname: Dremio hostname
            port: REST API port (auto-detected based on mode if not provided)
            username: Username for authentication (Software with user/pass)
            password: Password for authentication (Software with user/pass)
            tls: Enable TLS/SSL
            disable_certificate_verification: Disable SSL certificate verification
            flight_port: Arrow Flight port (auto-detected based on mode if not provided)
            flight_endpoint: Arrow Flight endpoint (defaults to hostname if not provided)
            mode: Connection mode - 'cloud' (default), 'v26', or 'v25'
                  - 'cloud': Dremio Cloud (default)
                  - 'v26': Dremio Software v26+ with PAT support
                  - 'v25': Dremio Software v25 and earlier
        """
        
        self.mode = mode.lower()
        
        # Get credentials based on mode
        # Mode determines which environment variables to prioritize
        if self.mode == "cloud":
            # Cloud mode: use DREMIO_PAT and DREMIO_PROJECT_ID
            self.pat = pat or os.getenv("DREMIO_PAT")
            self.project_id = project_id or os.getenv("DREMIO_PROJECT_ID")
        elif self.mode in ["v26", "v25"]:
            # Software mode: use DREMIO_SOFTWARE_* variables
            self.pat = pat or os.getenv("DREMIO_SOFTWARE_PAT")
            self.project_id = None  # Explicitly None for Software
            # Override hostname from env if not provided
            if hostname == "data.dremio.cloud":  # Default wasn't changed
                env_host = os.getenv("DREMIO_SOFTWARE_HOST")
                if env_host:
                    # Extract hostname from URL if needed
                    hostname = env_host.replace("https://", "").replace("http://", "")
                    if ":" in hostname:
                        hostname = hostname.split(":")[0]
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'cloud', 'v26', or 'v25'")
        
        # Connection details
        self.hostname = hostname
        self._username = username
        self.password = password
        self.tls = tls
        self.disable_certificate_verification = disable_certificate_verification
        
        # Set smart defaults based on mode
        if self.mode == "cloud":
            self.port = port if port is not None else 443
            self.flight_port = flight_port if flight_port is not None else 443
            self.flight_endpoint = flight_endpoint or "data.dremio.cloud"
            if not base_url:
                self.base_url = "https://api.dremio.cloud/v0"
            else:
                self.base_url = base_url
                
        elif self.mode == "v26":
            # Dremio Software v26+ with PAT support
            # REST API typically on port 443 or 9047
            # Flight typically on port 32010
            self.port = port if port is not None else (443 if tls else 9047)
            self.flight_port = flight_port if flight_port is not None else 32010
            self.flight_endpoint = flight_endpoint or hostname
            if not base_url:
                protocol = "https" if tls else "http"
                self.base_url = f"{protocol}://{hostname}:{self.port}/api/v3"
            else:
                self.base_url = base_url
                
        elif self.mode == "v25":
            # Dremio Software v25 and earlier
            # REST API on port 9047, Flight on port 32010
            self.port = port if port is not None else 9047
            self.flight_port = flight_port if flight_port is not None else 32010
            self.flight_endpoint = flight_endpoint or hostname
            if not base_url:
                protocol = "https" if tls else "http"
                self.base_url = f"{protocol}://{hostname}:9047/api/v3"
            else:
                self.base_url = base_url
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'cloud', 'v26', or 'v25'")

        # Validation
        if not self.pat and not (self.username and self.password):
            raise ValueError("Either PAT or Username/Password is required.")
        
        self.session = requests.Session()
        if self.pat:
            self.session.headers.update({"Authorization": f"Bearer {self.pat}"})
        elif self.username and self.password:
            # For REST API, we need to login to get a token.
            # Dremio Software /apiv3/login (v26+) or /apiv2/login (v25)
            try:
                # Determine base root (remove /api/v3 if present)
                if self.base_url.endswith("/api/v3"):
                    base_root = self.base_url[:-7] # remove /api/v3
                else:
                    base_root = self.base_url

                # Endpoints to try based on mode
                if self.mode == "v26":
                    login_endpoints = [
                        f"{self.base_url}/login",       # Standard v3
                        f"{base_root}/apiv3/login"      # v26+ alternative
                    ]
                else:  # v25
                    login_endpoints = [
                        f"{base_root}/apiv2/login",     # v25
                        f"{self.base_url}/login"        # Fallback
                    ]

                token = None
                for login_url in login_endpoints:
                    try:
                        payload = {"userName": self.username, "password": self.password}
                        headers = {"Content-Type": "application/json", "Accept": "application/json"}
                        response = requests.post(login_url, json=payload, headers=headers, verify=not self.disable_certificate_verification)
                        
                        if response.status_code == 200:
                            try:
                                data = response.json()
                                token = data.get("token")
                                if token:
                                    self.session.headers.update({"Authorization": f"_dremio{token}"})
                                    # Also set self.pat so Flight client can use it as Bearer token
                                    self.pat = token
                                    break # Success
                            except Exception:
                                pass
                    except Exception:
                        pass
                
                if not token:
                    print("Warning: All REST API login attempts failed.")

            except Exception as e:
                print(f"Warning: REST API login process failed: {e}")
            
        self.session.headers.update({
            "Content-Type": "application/json"
        })

        # Lazy-loaded properties
        self._catalog = None
        self._admin = None
        self._udf = None
        self._iceberg = None
        
        # If mode is v26 and we have PAT but no username, we might need to discover it for Flight
        if self.mode == "v26" and self.pat and not self.username:
            # We don't block here, but we'll fetch it when accessed if still None
            pass

    @property
    def username(self):
        if self._username:
            return self._username
        
        # Try to discover username if not set (only for v26/Software modes where it's needed)
        if self.mode in ["v26", "v25"] and self.pat:
            try:
                self._username = self._discover_username()
            except Exception:
                pass # Return None if discovery fails
        
        return self._username

    @username.setter
    def username(self, value):
        self._username = value

    def _discover_username(self):
        """
        Attempt to discover the username from the catalog.
        Useful for v26+ where we might only have a PAT.
        """
        try:
            # We need to use the internal _catalog property or create a temporary one
            # to avoid circular dependency if Catalog needs client.username (it shouldn't)
            # But client.catalog property initializes Catalog(self)
            
            # Simple REST call to list catalog
            response = self.session.get(f"{self.base_url}/catalog")
            if response.status_code == 200:
                data = response.json()
                # data is { "data": [ ... ] }
                items = data.get("data", [])
                for item in items:
                    path = item.get("path", [])
                    if path and path[0].startswith("@"):
                        return path[0][1:] # Remove @ prefix
            return None
        except Exception:
            return None

    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = Catalog(self)
        return self._catalog

    @property
    def admin(self):
        if self._admin is None:
            self._admin = Admin(self)
        return self._admin

    @property
    def udf(self):
        if self._udf is None:
            self._udf = UDFManager(self)
        return self._udf

    @property
    def iceberg(self):
        if self._iceberg is None:
            from .iceberg import DremioIcebergClient
            self._iceberg = DremioIcebergClient(self)
        return self._iceberg

    def table(self, path: str) -> DremioBuilder:
        return DremioBuilder(self, path)

    def sql(self, query: str):
        # This will return a builder initialized with a SQL query or directly execute it.
        # For now, let's return a builder that can execute raw SQL.
        return DremioBuilder(self, sql=query)

    def execute(self, query: str, format: str = "pandas"):
        """Execute raw SQL query directly via Flight"""
        # Create a temporary builder just to access _execute_flight
        # Or better, move _execute_flight to client or utils?
        # For now, just use a builder
        return DremioBuilder(self)._execute_flight(query, format)

    def query(self, sql: str, format: str = "pandas"):
        """
        Execute a raw SQL query and return the result.
        
        Args:
            sql: The SQL query to execute.
            format: The return format ('pandas', 'arrow', 'polars').
        
        Returns:
            DataFrame or Table in the requested format.
        """
        return DremioBuilder(self, sql=sql).collect(format)

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

    def list_files(self, path: str) -> DremioBuilder:
        """
        Query the LIST_FILES table function for a given path.
        Useful for accessing unstructured data.
        """
        return DremioBuilder(self, f"TABLE(LIST_FILES('{path}'))")

    def upload_file(self, file_path: str, table_name: str, file_format: str = None, **kwargs):
        """
        Upload a local file to Dremio as a new table.
        
        Args:
            file_path: Path to the local file.
            table_name: Destination table name (e.g., "space.folder.table").
            file_format: 'csv', 'json', 'parquet', 'excel', 'html', 'avro', 'orc', 'lance', 'feather'. 
                         If None, inferred from extension.
            **kwargs: Additional arguments passed to the file reader.
        """
        import pyarrow as pa
        import pyarrow.csv as csv
        import pyarrow.json as json
        import pyarrow.parquet as parquet
        import os

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_format is None:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                file_format = 'csv'
            elif ext == '.json':
                file_format = 'json'
            elif ext == '.parquet':
                file_format = 'parquet'
            elif ext in ['.xlsx', '.xls', '.ods']:
                file_format = 'excel'
            elif ext == '.html':
                file_format = 'html'
            elif ext == '.avro':
                file_format = 'avro'
            elif ext == '.orc':
                file_format = 'orc'
            elif ext == '.lance':
                file_format = 'lance'
            elif ext in ['.feather', '.arrow']:
                file_format = 'feather'
            else:
                raise ValueError(f"Could not infer format from extension {ext}. Please specify file_format.")

        table = None

        if file_format == 'csv':
            table = csv.read_csv(file_path, **kwargs)
        elif file_format == 'json':
            table = json.read_json(file_path, **kwargs)
        elif file_format == 'parquet':
            table = parquet.read_table(file_path, **kwargs)
        elif file_format == 'excel':
            try:
                import pandas as pd
                df = pd.read_excel(file_path, **kwargs)
                table = pa.Table.from_pandas(df)
            except ImportError:
                raise ImportError("pandas and openpyxl are required for Excel files. Install with `pip install pandas openpyxl`.")
        elif file_format == 'html':
            try:
                import pandas as pd
                # read_html returns a list of DataFrames
                dfs = pd.read_html(file_path, **kwargs)
                if not dfs:
                    raise ValueError("No tables found in HTML file.")
                # Default to the first table, or user can pass 'match' in kwargs to filter
                table = pa.Table.from_pandas(dfs[0])
            except ImportError:
                raise ImportError("pandas and lxml/html5lib are required for HTML files. Install with `pip install pandas lxml`.")
        elif file_format == 'avro':
            try:
                import fastavro
                with open(file_path, 'rb') as f:
                    reader = fastavro.reader(f)
                    records = list(reader)
                    table = pa.Table.from_pylist(records)
            except ImportError:
                raise ImportError("fastavro is required for Avro files. Install with `pip install fastavro`.")
        elif file_format == 'orc':
            try:
                import pyarrow.orc as orc
                table = orc.read_table(file_path, **kwargs)
            except ImportError:
                raise ImportError("pyarrow.orc is required for ORC files.")
        elif file_format == 'lance':
            try:
                import lance
                ds = lance.dataset(file_path)
                table = ds.to_table(**kwargs)
            except ImportError:
                raise ImportError("lance is required for Lance files. Install with `pip install pylance`.")
        elif file_format == 'feather':
            try:
                import pyarrow.feather as feather
                table = feather.read_table(file_path, **kwargs)
            except ImportError:
                raise ImportError("pyarrow is required for Feather files.")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        # Create table and insert data
        self.table(table_name).create(table_name, data=table)
        print(f"Successfully uploaded {file_path} to {table_name}")
    @property
    def ingest(self):
        """
        Access ingestion modules.
        Example: client.ingest.dlt(source, "table")
        """
        from dremioframe import ingest
        
        class IngestNamespace:
            def __init__(self, client):
                self.client = client
                
            def dlt(self, source, table_name, **kwargs):
                return ingest.ingest_dlt(self.client, source, table_name, **kwargs)

            def database(self, connection_string, query, table_name, **kwargs):
                return ingest.ingest_database(self.client, connection_string, query, table_name, **kwargs)

            def files(self, pattern, table_name, **kwargs):
                return ingest.ingest_files(self.client, pattern, table_name, **kwargs)
                
        return IngestNamespace(self)
