# Creating Semantic Views

The Semantic Layer is the interface between your physical data (tables, files) and your business users (BI tools, analysts). In Dremio, this is implemented using **Virtual Datasets (Views)**.

## Creating Views

Use `catalog.create_view` to define business logic programmatically.

### Example: Customer 360 View

You can pass a SQL string or a `DremioBuilder` object.

```python
# Option 1: Using SQL String
client.catalog.create_view(
    path=["marketing", "customer_360"],
    sql="SELECT * FROM source.table"
)

# Option 2: Using DremioBuilder (DataFrame API)
# Define logic using the DataFrame API
df = client.table("gold.dim_users").alias("u") \
    .join(client.table("gold.fact_orders").alias("o"), on="u.user_id = o.user_id", how="left") \
    .group_by("u.user_id", "u.email") \
    .agg(total_orders=F.count("o.order_id"))

# Create view from the dataframe definition
client.catalog.create_view(
    path=["marketing", "customer_360_v2"],
    sql=df
)
```

## Best Practices

### 1. Business-Friendly Naming
- Use clear, descriptive names for views and columns.
- Avoid technical jargon or abbreviations (e.g., use `customer_id` instead of `c_id`).
- Rename columns to match business terminology.

### 2. Pre-Calculate Metrics
- Aggregate common metrics (e.g., `total_sales`, `avg_order_value`) in the view to ensure consistency across all BI tools.
- Encapsulate complex logic (e.g., "Active User" definition) within the view.

### 3. Star Schema Abstraction
- Join Fact and Dimension tables in the view so users don't have to perform complex joins themselves.
- Present a "wide" table that is easy to filter and group.

### 4. Security
- Use Row-Level and Column-Level permissions (if available) or create specific views for different user groups to restrict access to sensitive data.

## Version Control

Since Views are defined by SQL, you can version control their definitions in Git.

1.  Store the SQL definition in a `.sql` file in your repo.
2.  Use a CI/CD pipeline (using DremioFrame) to deploy the view when the SQL file changes.

```python
# CI/CD Script Example
import glob

# Deploy all views in the 'views/marketing' directory
for sql_file in glob.glob("views/marketing/*.sql"):
    view_name = sql_file.split("/")[-1].replace(".sql", "")
    with open(sql_file, "r") as f:
        sql = f.read()
    
    print(f"Deploying {view_name}...")
    client.catalog.create_view(
        path=["marketing", view_name],
        sql=sql
    )
```
