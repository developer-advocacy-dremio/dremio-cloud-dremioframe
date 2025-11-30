import typer
import os
from rich.console import Console
from rich.table import Table
from dremioframe.client import DremioClient

app = typer.Typer()
console = Console()

def get_client():
    pat = os.getenv("DREMIO_PAT")
    url = os.getenv("DREMIO_URL")
    project_id = os.getenv("DREMIO_PROJECT_ID")
    
    if not pat:
        console.print("[red]Error: DREMIO_PAT environment variable not set.[/red]")
        raise typer.Exit(code=1)
        
    return DremioClient(pat=pat, hostname=url, project_id=project_id)

@app.command()
def query(sql: str):
    """Run a SQL query."""
    client = get_client()
    try:
        df = client.sql(sql).collect("pandas")
        console.print(df.to_markdown(index=False))
    except Exception as e:
        console.print(f"[red]Query failed: {e}[/red]")

@app.command()
def catalog(path: str = None):
    """List catalog items."""
    client = get_client()
    try:
        items = client.catalog.list_catalog(path)
        table = Table(title=f"Catalog: {path or 'Root'}")
        table.add_column("Name")
        table.add_column("Type")
        table.add_column("ID")
        
        for item in items:
            table.add_row(item.get("path", [""])[-1], item.get("type"), item.get("id"))
            
        console.print(table)
    except Exception as e:
        console.print(f"[red]Failed to list catalog: {e}[/red]")

@app.command()
def reflections():
    """List all reflections."""
    client = get_client()
    try:
        refs = client.admin.list_reflections()
        table = Table(title="Reflections")
        table.add_column("Name")
        table.add_column("Type")
        table.add_column("Status")
        table.add_column("Dataset ID")
        
        for r in refs.get("data", []):
            status = "Enabled" if r.get("enabled") else "Disabled"
            table.add_row(r.get("name"), r.get("type"), status, r.get("datasetId"))
            
        console.print(table)
    except Exception as e:
        console.print(f"[red]Failed to list reflections: {e}[/red]")

pipeline_app = typer.Typer()
app.add_typer(pipeline_app, name="pipeline", help="Manage orchestration pipelines.")

@pipeline_app.command("list")
def list_pipelines(
    backend_url: str = typer.Option("sqlite:///dremioframe.db", help="Backend connection string (e.g. sqlite:///path.db)"),
):
    """List all pipelines (requires connecting to backend)."""
    # Note: This lists RUNS, not pipelines definitions, because definitions are in code.
    # To list definitions, we'd need to import the user's code.
    # So instead, let's list recent runs from the backend.
    
    # We need to instantiate the backend based on the URL.
    # For simplicity in CLI, let's support SQLite and Postgres via DSN.
    from dremioframe.orchestration.backend import SQLiteBackend, PostgresBackend, MySQLBackend
    
    backend = None
    if backend_url.startswith("sqlite:///"):
        path = backend_url.replace("sqlite:///", "")
        backend = SQLiteBackend(path)
    elif backend_url.startswith("postgresql://"):
        backend = PostgresBackend(dsn=backend_url)
    elif backend_url.startswith("mysql://"):
        # Parsing mysql url is harder, let's assume env vars or basic support
        console.print("[yellow]MySQL via CLI URL is experimental. Use env vars.[/yellow]")
        # For now, just try to init if env vars are set, ignoring URL if it's just 'mysql://'
        backend = MySQLBackend()
    else:
        console.print(f"[red]Unsupported backend URL: {backend_url}[/red]")
        raise typer.Exit(1)
        
    try:
        runs = backend.list_runs(limit=10)
        table = Table(title="Recent Pipeline Runs")
        table.add_column("Pipeline")
        table.add_column("Run ID")
        table.add_column("Status")
        table.add_column("Start Time")
        
        for run in runs:
            table.add_row(run.pipeline_name, run.run_id, run.status, str(run.start_time))
            
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing runs: {e}[/red]")

@pipeline_app.command("ui")
def start_ui_cmd(
    port: int = 8080,
    backend_url: str = typer.Option("sqlite:///dremioframe.db", help="Backend connection string"),
):
    """Start the Orchestration UI."""
    from dremioframe.orchestration.ui import start_ui
    from dremioframe.orchestration.backend import SQLiteBackend, PostgresBackend, MySQLBackend
    
    backend = None
    if backend_url.startswith("sqlite:///"):
        path = backend_url.replace("sqlite:///", "")
        backend = SQLiteBackend(path)
    elif backend_url.startswith("postgresql://"):
        backend = PostgresBackend(dsn=backend_url)
    # ... (similar logic for others)
    else:
        # Default to SQLite if path provided without prefix? No, be strict.
        if "://" not in backend_url:
             backend = SQLiteBackend(backend_url)
        else:
             console.print(f"[red]Unsupported backend URL: {backend_url}[/red]")
             raise typer.Exit(1)
             
    console.print(f"[green]Starting UI on port {port}...[/green]")
    start_ui(backend, port=port)

dq_app = typer.Typer()
app.add_typer(dq_app, name="dq", help="Run Data Quality tests.")

@dq_app.command("run")
def run_dq_tests(
    directory: str = typer.Argument(..., help="Directory containing YAML test files."),
):
    """Run data quality tests from YAML files."""
    try:
        from dremioframe.dq.runner import DQRunner
    except ImportError:
        console.print("[red]DQ dependencies not installed. Run `pip install dremioframe[dq]`[/red]")
        raise typer.Exit(1)
        
    client = get_client()
    runner = DQRunner(client)
    
    try:
        tests = runner.load_tests(directory)
        if not tests:
            console.print(f"[yellow]No tests found in {directory}[/yellow]")
            return
            
        success = runner.run_tests(tests)
        if not success:
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[red]Error running tests: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def repl():
    """Start an interactive Dremio shell."""
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
        from prompt_toolkit.lexers import PygmentsLexer
        from pygments.lexers.sql import SqlLexer
    except ImportError:
        console.print("[red]REPL dependencies not installed. Run `pip install dremioframe[cli]`[/red]")
        raise typer.Exit(1)
        
    client = get_client()
    console.print("[green]Welcome to DremioFrame Shell![/green]")
    console.print("Type 'exit' or 'quit' to leave.")
    
    session = PromptSession(
        history=FileHistory('.dremio_history'),
        auto_suggest=AutoSuggestFromHistory(),
        lexer=PygmentsLexer(SqlLexer)
    )
    
    while True:
        try:
            text = session.prompt('dremio> ')
            text = text.strip()
            
            if not text:
                continue
                
            if text.lower() in ['exit', 'quit']:
                break
                
            if text.lower() == 'tables':
                # Quick list of tables in current context or root
                # We don't have a "current context" in client easily unless we track it.
                # Just list root catalog
                try:
                    items = client.catalog.list_catalog()
                    table = Table(title="Root Catalog")
                    table.add_column("Name")
                    table.add_column("Type")
                    for item in items:
                        table.add_row(item.get("path", [""])[-1], item.get("type"))
                    console.print(table)
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                continue
                
            # Assume SQL
            try:
                # Detect if it's a SELECT or DML
                # client.execute handles DML (returns affected rows)
                # client.query handles SELECT (returns DataFrame)
                # But client.execute works for SELECT too (returns Polars)
                # Let's use client.query with pandas for display
                
                # If it doesn't start with SELECT, maybe use execute?
                # But client.query uses Flight which handles both usually.
                
                df = client.query(text, format="pandas")
                console.print(df.to_markdown(index=False))
                
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                
        except KeyboardInterrupt:
            continue
        except EOFError:
            break
    
    console.print("Goodbye!")

if __name__ == "__main__":
    app()
