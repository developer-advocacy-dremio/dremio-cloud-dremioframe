# Joins

DremioFrame allows you to join tables or builder instances.

## Join Syntax

```python
left_df.join(other, on, how='inner')
```

- **other**: Can be a table name (string) or another `DremioBuilder` object.
- **on**: The join condition (SQL string).
- **how**: Join type (`inner`, `left`, `right`, `full`, `cross`).

## Examples

### Join with a Table Name

```python
df = client.table("orders")
joined = df.join("customers", on="left_tbl.customer_id = right_tbl.id", how="left")
joined.show()
```

### Join with Another Builder

This allows you to filter or transform the right-side table before joining.

```python
orders = client.table("orders")
customers = client.table("customers").filter("active = true")

joined = orders.join(customers, on="left_tbl.customer_id = right_tbl.id")
joined.show()
```

### Note on Aliases

When joining, DremioFrame automatically wraps the left and right sides in subqueries aliased as `left_tbl` and `right_tbl` respectively. You should use these aliases in your `on` condition.
