# DremioFrame Cookbook

A collection of recipes for common data engineering tasks using valid Dremio SQL patterns.

## Deduplicating Data

Remove duplicate records based on specific columns, keeping the most recent one.

```python
# Keep the row with the latest 'updated_at' for each 'id'
df = client.table("source_table") \
    .select("*", 
            F.row_number().over(
                F.Window.partition_by("id").order_by("updated_at", ascending=False)
            ).alias("rn")
    ) \
    .filter("rn = 1") \
    .drop("rn")

# Save as new table
df.create("deduplicated_table")
```

## Pivoting Data

Transform rows into columns (e.g., monthly sales).

```python
# Source: region, month, sales
# Target: region, jan_sales, feb_sales, ...

df = client.table("monthly_sales") \
    .group_by("region") \
    .agg(
        jan_sales="SUM(CASE WHEN month = 'Jan' THEN sales ELSE 0 END)",
        feb_sales="SUM(CASE WHEN month = 'Feb' THEN sales ELSE 0 END)",
        mar_sales="SUM(CASE WHEN month = 'Mar' THEN sales ELSE 0 END)"
    )

df.show()
```

## Incremental Loading (Watermark)

Load only new data based on a watermark (max timestamp).

```python
# Get max timestamp from target
max_ts = client.table("target_table").agg(max_ts="MAX(updated_at)").collect().iloc[0]['max_ts']

# Fetch only new data from source
new_data = client.table("source_table").filter(f"updated_at > '{max_ts}'")

# Append to target
new_data.insert("target_table")
```

## Exporting to S3 (Parquet)

```python
# Option 1: Client-Side Export (Requires local credentials & s3fs)
# Fetches data to client and writes to S3
df = client.table("warehouse.sales").collect()
df.to_parquet("s3://my-bucket/export/data.parquet")

# Option 2: Materialize to Source (CTAS)
# Creates a new table (Iceberg or Parquet folder) in the S3 source
client.sql("""
    CREATE TABLE "s3_source"."bucket"."folder"."new_table"
    AS SELECT * FROM "warehouse"."sales"
""")
```

## Handling JSON Data

Access nested fields in JSON/Struct columns using dot notation.

```python
# Source has a 'details' column (Struct or Map): {"color": "red", "size": "M"}
df = client.table("products") \
    .select(
        "id",
        "name",
        F.col("details.color").alias("color"),
        F.col("details.size").alias("size")
    )

df.show()
```

## Unnesting Arrays (Flatten)

Explode a list column into multiple rows.

```python
# Source: id, tags (["A", "B"])
# Target: id, tag (one row per tag)

df = client.table("posts") \
    .select(
        "id",
        F.flatten("tags").alias("tag")
    )

df.show()
```

## Date Arithmetic

Perform calculations on dates.

```python
# Calculate deadline (created_at + 7 days) and days_overdue
df = client.table("tasks") \
    .select(
        "id",
        "created_at",
        F.date_add("created_at", 7).alias("deadline"),
        F.date_diff(F.current_date(), "created_at").alias("days_since_creation")
    )

df.show()
```

## String Manipulation

Clean and transform text data.

```python
# Normalize email addresses
df = client.table("users") \
    .select(
        "id",
        F.lower(F.trim("email")).alias("clean_email"),
        F.substr("phone", 1, 3).alias("area_code")
    )

df.show()
```

## Window Functions (Running Total)

Calculate cumulative sums or moving averages.

```python
# Calculate running total of sales by date
df = client.table("sales") \
    .select(
        "date",
        "amount",
        F.sum("amount").over(
            F.Window.order_by("date").rows_between("UNBOUNDED PRECEDING", "CURRENT ROW")
        ).alias("running_total")
    )

df.show()
```

## Approximate Count Distinct

Estimate the number of distinct values for large datasets (faster than COUNT DISTINCT).

```python
# Estimate unique visitors
df = client.table("web_logs") \
    .agg(
        unique_visitors=F.approx_distinct("visitor_id")
    )

df.show()
```

## AI Functions (Generative AI)

Use Dremio's AI functions to classify text or generate content.

```python
# Classify customer feedback
df = client.table("feedback") \
    .select(
        "comment",
        F.ai_classify("comment", ["Positive", "Negative", "Neutral"]).alias("sentiment")
    )

df.show()
```

## Time Travel (Snapshot Querying)

Query an Iceberg table as it existed at a specific point in time.

```python
# Query specific snapshot
df = client.table("iceberg_table").at_snapshot("1234567890")

# Query by timestamp
df = client.table("iceberg_table").at_timestamp("2023-10-27 10:00:00")

df.show()
```

## Schema Evolution

Add a new column to an existing Iceberg table.

```python
# Add 'status' column
client.sql('ALTER TABLE "iceberg_table" ADD COLUMNS (status VARCHAR)')
```

## Creating Partitioned Tables

Create a new table partitioned by specific columns for better performance.

```python
# Create table partitioned by 'region' and 'date'
client.sql("""
    CREATE TABLE "iceberg_source"."new_table"
    PARTITION BY (region, date)
    AS SELECT * FROM "source_table"
""")
```

## Map & Struct Access

Access values within Map and Struct data types.

```python
# Struct: details.color
# Map: properties['priority']

df = client.table("events") \
    .select(
        "id",
        F.col("details.color").alias("color"),
        F.col("properties['priority']").alias("priority")
    )

df.show()
```
