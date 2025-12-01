# Dimensional Modeling Guide

Dimensional modeling is a data structure technique optimized for data warehousing and reporting. It prioritizes **query performance** and **ease of use** for business analysts over the write-efficiency of normalized (3NF) schemas.

## Core Concepts

### 1. Star Schema vs. Snowflake Schema

*   **Star Schema (Recommended)**: A central Fact table surrounded by denormalized Dimension tables.
    *   **Pros**: Simpler queries (fewer joins), faster performance in Dremio (Starflake optimization).
    *   **Cons**: Redundant data in dimensions (e.g., repeating "USA" for every customer in New York).
*   **Snowflake Schema**: Dimensions are normalized into multiple related tables (e.g., `Fact_Sales` -> `Dim_Customer` -> `Dim_City` -> `Dim_Country`).
    *   **Pros**: Less data redundancy.
    *   **Cons**: Complex queries (many joins), slower performance.

> [!TIP]
> **Dremio Recommendation**: Prefer **Star Schemas**. Dremio's columnar nature handles the redundancy of denormalized dimensions efficiently (via compression), and fewer joins leads to better query planning.

### 2. Fact Tables

Fact tables store the quantitative data (metrics) of the business. They are typically the largest tables.

*   **Transactional Fact**: One row per event (e.g., a single sale, a web click). High volume, most granular.
*   **Periodic Snapshot Fact**: One row per entity per time period (e.g., Monthly Account Balance, Daily Inventory Level). Good for trend analysis.
*   **Accumulating Snapshot Fact**: One row per lifecycle of a process (e.g., Order Fulfillment: Order Date, Ship Date, Delivery Date). Good for calculating lag times.

### 3. Dimension Tables

Dimension tables provide the "who, what, where, when, and why" context to the facts.

*   **Conformed Dimension**: A dimension shared across multiple fact tables (e.g., `Dim_Date`, `Dim_Customer`). Crucial for cross-process analysis (Drill-Across).
*   **Junk Dimension**: A collection of low-cardinality flags and indicators combined into a single table to avoid cluttering the fact table.
*   **Degenerate Dimension**: A dimension key that has no associated attributes (e.g., `Transaction ID` in the Fact table).
*   **Role-Playing Dimension**: A single physical dimension table referenced multiple times for different purposes (e.g., `Dim_Date` used for `Order Date`, `Ship Date`, and `Delivery Date`).

---

## Implementation in DremioFrame

We recommend a **Bronze -> Silver -> Gold** workflow.
*   **Bronze**: Raw data ingestion.
*   **Silver**: Cleaned, deduplicated, and standardized data.
*   **Gold**: Dimensional models (Star Schemas) ready for BI.

### 1. Generating Surrogate Keys

While Dremio handles string joins well, integer surrogate keys are standard in data warehousing for decoupling from source system keys and handling Slowly Changing Dimensions (SCD).

**Strategy**: Use `row_number()` or a hash function if you don't have a sequence generator.

```python
# Creating a Dimension with a Surrogate Key
client.sql("""
    CREATE TABLE "gold"."dim_product" AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY product_id) as product_sk, -- Surrogate Key
        product_id as product_nk, -- Natural Key (from source)
        product_name,
        category,
        brand,
        current_date() as valid_from,
        NULL as valid_to,
        TRUE as is_current
    FROM "silver"."products"
""")
```

### 2. The Date Dimension (`Dim_Date`)

A robust Date Dimension is essential. Do not rely on SQL date functions alone; a table allows you to filter by "Fiscal Quarter", "Holiday", "Weekday/Weekend", etc.

**Generator Script**:

```python
import pandas as pd
from dremioframe.client import DremioClient

# 1. Generate Data in Pandas
start_date = "2020-01-01"
end_date = "2030-12-31"
dates = pd.date_range(start_date, end_date)

df = pd.DataFrame({"date_key": dates})
df["date_id"] = df["date_key"].dt.strftime("%Y%m%d").astype(int)
df["year"] = df["date_key"].dt.year
df["quarter"] = df["date_key"].dt.quarter
df["month"] = df["date_key"].dt.month
df["day_of_week"] = df["date_key"].dt.dayofweek + 1
df["day_name"] = df["date_key"].dt.day_name()
df["is_weekend"] = df["day_of_week"].isin([6, 7])

# 2. Write to Dremio (Iceberg)
client = DremioClient()
client.table("gold.dim_date").create("gold.dim_date", data=df)
```

### 3. Building the Fact Table

The Fact table joins Silver data with Dimensions to retrieve Surrogate Keys.

```python
client.sql("""
    CREATE TABLE "gold"."fact_sales" AS
    SELECT
        p.product_sk,
        c.customer_sk,
        d.date_id,
        s.transaction_id,
        s.quantity,
        s.amount
    FROM "silver"."sales" s
    JOIN "gold"."dim_product" p ON s.product_id = p.product_nk AND p.is_current = TRUE
    JOIN "gold"."dim_customer" c ON s.customer_id = c.customer_nk AND c.is_current = TRUE
    JOIN "gold"."dim_date" d ON s.sale_date = d.date_key
""")
```

---

## Performance Optimization

### 1. Partitioning (Iceberg)

Partition your **Fact Tables** by the main time-based filter field (usually `date_id` or `transaction_date`).

*   **Small/Medium Data**: Partition by `Month` or `Year`.
*   **Large Data**: Partition by `Day`.

```python
# Create table with partition transform
client.sql("""
    CREATE TABLE "gold"."fact_sales" (
        ...
    ) PARTITION BY (day(transaction_date))
""")
```

> [!WARNING]
> Avoid high-cardinality partitions (e.g., partitioning by `User ID` or `Timestamp`). This creates too many small files (Small File Problem).

### 2. Sorting

Sorting data before writing improves file pruning (Min/Max skipping). Sort Fact tables by the columns most frequently used in `WHERE` clauses (e.g., `customer_id`, `region`).

```python
# In DremioFrame Builder
client.table("source").sort("region", "transaction_date").create("gold.fact_sales")
```

### 3. Aggregation Reflections

For Star Schemas, **Aggregation Reflections** are the most powerful optimization. They pre-calculate aggregates across dimensions.

**Best Practice**: Create an Aggregation Reflection on the **Fact Table**.
*   **Dimensions**: Add the Foreign Keys (e.g., `product_sk`, `customer_sk`, `date_id`).
*   **Measures**: Add the metrics (e.g., `SUM(amount)`, `COUNT(*)`).

```python
client.admin.create_reflection(
    dataset_id="fact_sales_uuid",
    name="agg_sales_by_keys",
    type="AGGREGATION",
    dimension_fields=["product_sk", "customer_sk", "date_id"],
    measure_fields=["amount", "quantity"]
)
```

When a user queries `JOIN dim_product ... GROUP BY category`, Dremio automatically substitutes the reflection, avoiding the scan of the raw Fact table.
