# Dimensional Modeling

Dimensional modeling is a data structure technique optimized for data warehousing and reporting. The most common implementation is the **Star Schema**, which consists of **Fact** tables referencing **Dimension** tables.

## Concepts

### Fact Tables
Fact tables store quantitative data for analysis and are often very large. They contain:
- **Measurements/Metrics**: Numeric values (e.g., Sales Amount, Quantity).
- **Foreign Keys**: Links to dimension tables (e.g., Product ID, Customer ID).

### Dimension Tables
Dimension tables store descriptive attributes about the facts. They are typically smaller and denormalized. They contain:
- **Primary Key**: Unique identifier.
- **Attributes**: Descriptive text (e.g., Product Name, Category, Customer City).

## Implementing in DremioFrame

You can build Fact and Dimension tables in your **Gold** layer using DremioFrame.

### Creating a Dimension Table

Dimensions are often derived from Silver layer tables by selecting distinct entities and their latest attributes.

```python
# Create Product Dimension
# Assumes 'silver.products' is a clean list of products
client.sql("""
    CREATE TABLE "gold"."dim_products" AS
    SELECT 
        product_id,
        product_name,
        category,
        brand,
        unit_price,
        CURRENT_DATE as load_date
    FROM "silver"."products"
""")
```

### Creating a Fact Table

Fact tables are created by joining transactional data with dimensions (to ensure referential integrity, though Dremio doesn't enforce it) and selecting keys and metrics.

```python
# Create Sales Fact
client.sql("""
    CREATE TABLE "gold"."fact_sales" AS
    SELECT 
        s.transaction_id,
        s.product_id,   -- FK to dim_products
        s.customer_id,  -- FK to dim_customers
        s.store_id,     -- FK to dim_stores
        s.transaction_date,
        s.quantity,
        s.amount,
        (s.quantity * s.amount) as total_amount
    FROM "silver"."sales_transactions" s
""")
```

## Querying Star Schemas

Dremio is highly optimized for Star Schema queries (Starflake joins).

```python
from dremioframe import F

# Analyze Sales by Product Category
df = client.table("gold.fact_sales").alias("f") \
    .join(
        client.table("gold.dim_products").alias("p"),
        on="f.product_id = p.product_id"
    ) \
    .group_by("p.category") \
    .agg(
        total_sales=F.sum("f.total_amount"),
        avg_transaction=F.avg("f.amount")
    ) \
    .sort("total_sales", ascending=False)

df.show()
```

## Best Practices

1.  **Surrogate Keys**: Consider using surrogate keys (integers) for joins instead of natural keys (strings) for performance, though Dremio handles string joins efficiently.
2.  **Denormalization**: Don't be afraid to denormalize dimensions (e.g., include Category in the Product dimension rather than a separate Category dimension) to reduce joins (Snowflake vs Star schema).
3.  **Date Dimension**: Always create a comprehensive Date Dimension (`dim_date`) for time-based analysis (Year, Quarter, Month, Day of Week, Holiday, etc.).
