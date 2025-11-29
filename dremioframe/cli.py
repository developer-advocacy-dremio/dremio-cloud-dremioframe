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

if __name__ == "__main__":
    app()
