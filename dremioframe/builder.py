from typing import Optional, List, Any, Union, Dict, Any
import pyarrow.flight as flight
import pandas as pd
import polars as pl
import os
from .quality import DataQuality

class DremioBuilder:
    def __init__(self, client, path: Optional[str] = None, sql: Optional[str] = None):
        self.client = client
        self.path = path
        self.initial_sql = sql
        self.select_columns = []
        self.mutations = {}
        self.filters = []
        self.limit_val = None
        self.group_cols = []
        self.aggregations = {}
        self.order_cols = []
        self.distinct_flag = False
        self._quality = None

        self.time_travel_clause = ""

    @property
    def quality(self):
        if self._quality is None:
            self._quality = DataQuality(self)
        return self._quality

    def at_snapshot(self, snapshot_id: str) -> 'DremioBuilder':
        """Query at a specific snapshot ID"""
        self.time_travel_clause = f"AT SNAPSHOT '{snapshot_id}'"
        return self

    def at_timestamp(self, timestamp: str) -> 'DremioBuilder':
        """Query at a specific timestamp"""
        self.time_travel_clause = f"AT TIMESTAMP '{timestamp}'"
        return self

    def at_branch(self, branch: str) -> 'DremioBuilder':
        """Query at a specific branch"""
        self.time_travel_clause = f"AT BRANCH {branch}"
        return self

    def optimize(self, rewrite_data: bool = True, min_input_files: int = None) -> Any:
        """Run OPTIMIZE TABLE command"""
        if not self.path:
            raise ValueError("Optimize requires a table path")
        
        options = []
        if rewrite_data:
            options.append("REWRITE DATA")
        if min_input_files:
            options.append(f"(MIN_INPUT_FILES={min_input_files})")
            
        opt_sql = f"OPTIMIZE TABLE {self.path} {' '.join(options)}"
        return self._execute_dml(opt_sql)

    def vacuum(self, expire_snapshots: bool = True, retain_last: int = None) -> Any:
        """Run VACUUM TABLE command"""
        if not self.path:
            raise ValueError("Vacuum requires a table path")
            
        options = []
        if expire_snapshots:
            options.append("EXPIRE SNAPSHOTS")
        if retain_last:
            options.append(f"RETAIN_LAST {retain_last}")
            
        vac_sql = f"VACUUM TABLE {self.path} {' '.join(options)}"
        return self._execute_dml(vac_sql)
        
    def select(self, *columns: str) -> 'DremioBuilder':
        self.select_columns.extend(columns)
        return self

    def distinct(self) -> 'DremioBuilder':
        """Select distinct rows"""
        self.distinct_flag = True
        return self

    def group_by(self, *columns: str) -> 'DremioBuilder':
        """Group by columns"""
        self.group_cols.extend(columns)
        return self

    def agg(self, **kwargs) -> 'DremioBuilder':
        """
        Add aggregations.
        Example: df.group_by("state").agg(avg_pop="AVG(pop)")
        """
        self.aggregations.update(kwargs)
        return self

    def order_by(self, *columns: str, ascending: bool = True) -> 'DremioBuilder':
        """
        Order by columns.
        Example: df.order_by("col1", "col2", ascending=False)
        """
        direction = "ASC" if ascending else "DESC"
        for col in columns:
            self.order_cols.append(f"{col} {direction}")
        return self

    def mutate(self, **kwargs) -> 'DremioBuilder':
        """
        Add calculated columns.
        Example: df.mutate(new_col="col1 + col2")
        """
        self.mutations.update(kwargs)
        return self

    def filter(self, condition: str) -> 'DremioBuilder':
        self.filters.append(condition)
        return self

    def limit(self, n: int) -> 'DremioBuilder':
        self.limit_val = n
        return self

    def _compile_sql(self) -> str:
        if self.initial_sql:
            # If started with raw SQL, we wrap it in a subquery to apply further operations
            query = f"SELECT * FROM ({self.initial_sql}) AS sub"
        else:
            # Quote the path components if needed, or assume user passed a valid path string
            # Dremio paths are usually "Space"."Folder"."Table"
            query = f"SELECT * FROM {self.path}"
            if self.time_travel_clause:
                query += f" {self.time_travel_clause}"

        # Handle SELECT clause
        select_clause = "SELECT"
        if self.distinct_flag:
            select_clause += " DISTINCT"
        
        cols = []
        if self.select_columns:
            cols.extend(self.select_columns)
        
        if self.mutations:
            for name, expr in self.mutations.items():
                cols.append(f"{expr} AS {name}")
        
        if self.aggregations:
            for name, expr in self.aggregations.items():
                cols.append(f"{expr} AS {name}")
                
        # Implicitly add group columns to select if we are grouping
        if self.group_cols:
            for col in self.group_cols:
                # Check if col is already in cols (simple check)
                # This is imperfect but handles the common case
                if col not in self.select_columns:
                     cols.insert(0, col)

        if not cols:
            cols = ["*"]
            
        cols_str = ", ".join(cols)
        query = query.replace("SELECT *", f"{select_clause} {cols_str}", 1)

        if self.filters:
            where_clause = " AND ".join(self.filters)
            query += f" WHERE {where_clause}"
            
        if self.group_cols:
            group_clause = ", ".join(self.group_cols)
            query += f" GROUP BY {group_clause}"
            
        if self.order_cols:
            order_clause = ", ".join(self.order_cols)
            query += f" ORDER BY {order_clause}"

        if self.limit_val is not None:
            query += f" LIMIT {self.limit_val}"

        return query

    def join(self, other: Union[str, 'DremioBuilder'], on: str, how: str = "inner") -> 'DremioBuilder':
        """
        Join with another table or builder.
        
        Args:
            other: Table name string or DremioBuilder instance.
            on: Join condition (e.g., "t1.id = t2.id").
            how: Join type ("inner", "left", "right", "full", "cross").
        """
        # Compile self
        left_sql = self._compile_sql()
        
        # Compile other
        if isinstance(other, DremioBuilder):
            right_sql = other._compile_sql()
        else:
            right_sql = f"SELECT * FROM {other}"
            
        # Construct join
        # We wrap both in subqueries to ensure isolation
        # SELECT * FROM (left) AS left_tbl JOIN (right) AS right_tbl ON ...
        
        join_type = how.upper()
        if join_type not in ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]:
            raise ValueError(f"Invalid join type: {how}")
            
        join_sql = f"SELECT * FROM ({left_sql}) AS left_tbl {join_type} JOIN ({right_sql}) AS right_tbl ON {on}"
        
        # Return new builder with this SQL
        return DremioBuilder(self.client, sql=join_sql)

    def collect(self, library: str = "polars") -> Union[pl.DataFrame, pd.DataFrame]:
        sql = self._compile_sql()
        return self._execute_flight(sql, library)

    def _execute_flight(self, sql: str, library: str) -> Union[pl.DataFrame, pd.DataFrame]:
        # Construct Flight Endpoint
        protocol = "grpc+tls" if self.client.tls else "grpc+tcp"
        location = f"{protocol}://{self.client.hostname}:{self.client.port}"
        
        client = flight.FlightClient(location)
        
        # Authentication
        options = flight.FlightCallOptions()
        
        if self.client.pat:
            # Token Auth (Cloud or Software with PAT)
            headers = [
                (b"authorization", f"Bearer {self.client.pat}".encode("utf-8"))
            ]
            options = flight.FlightCallOptions(headers=headers)
        elif self.client.username and self.client.password:
            # Basic Auth (Software)
            # client.authenticate_basic_token(user, pass) returns call_options with the token
            options = client.authenticate_basic_token(self.client.username, self.client.password)
        
        if self.client.disable_certificate_verification:
            # This is usually set at client creation or context, but PyArrow Flight handles it via URI or args.
            # For disabling cert verification, we might need to pass generic options.
            # But let's assume the user handles certs via system trust store or explicit path if needed.
            pass

        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        reader = client.do_get(info.endpoints[0].ticket, options)
        table = reader.read_all()
        
        if library == "polars":
            return pl.from_arrow(table)
        elif library == "pandas":
            return table.to_pandas()
        else:
            raise ValueError("Library must be 'polars' or 'pandas'")

    def show(self, n: int = 20):
        print(self.limit(n).collect())

    def to_csv(self, path: str, **kwargs):
        """Export query results to CSV"""
        df = self.collect("pandas")
        df.to_csv(path, **kwargs)

    def to_parquet(self, path: str, **kwargs):
        """Export query results to Parquet"""
        df = self.collect("pandas")
        df.to_parquet(path, **kwargs)

    def chart(self, kind: str = 'line', x: str = None, y: str = None, title: str = None, save_to: str = None, **kwargs):
        """
        Create a chart from the query results using Matplotlib/Pandas.
        
        Args:
            kind: The kind of plot to produce: 'line', 'bar', 'barh', 'hist', 'box', 'kde', 'density', 'area', 'pie', 'scatter', 'hexbin'.
            x: Column name for x-axis.
            y: Column name(s) for y-axis.
            title: Chart title.
            save_to: Path to save the chart image (e.g., "chart.png").
            **kwargs: Additional arguments passed to df.plot().
        """
        import matplotlib.pyplot as plt
        
        df = self.collect("pandas")
        
        ax = df.plot(kind=kind, x=x, y=y, title=title, **kwargs)
        
        if save_to:
            plt.savefig(save_to)
            
        return ax

    # DML Operations
    def create(self, name: str, data: Union[Any, None] = None, batch_size: Optional[int] = None):
        """
        Create table as select (CTAS).
        If data is provided, it creates the table from the data (VALUES).
        """
        if data is not None:
            # Reuse insert logic to generate VALUES clause
            # But we need to wrap it in a SELECT * FROM (VALUES ...)
            # And we need to handle batching? CTAS usually is one shot.
            # If batching is needed, we should CREATE with first batch, then INSERT rest.
            
            import pyarrow as pa
            import math
            
            if isinstance(data, pd.DataFrame):
                data = pa.Table.from_pandas(data)
            
            if not isinstance(data, pa.Table):
                raise ValueError("Data must be a PyArrow Table or Pandas DataFrame")
            
            rows = data.to_pylist()
            total_rows = len(rows)
            
            # If batch_size is set, we use the first batch for CTAS
            first_batch_size = batch_size if batch_size else total_rows
            first_batch_rows = rows[:first_batch_size]
            
            # Generate VALUES for first batch
            values_list = []
            for row in first_batch_rows:
                row_vals = []
                for val in row.values():
                    if isinstance(val, str):
                        row_vals.append(f"'{val}'")
                    elif val is None:
                        row_vals.append("NULL")
                    else:
                        row_vals.append(str(val))
                values_list.append(f"({', '.join(row_vals)})")
            
            values_clause = ", ".join(values_list)
            cols_def = ", ".join(data.column_names)
            
            # CTAS SQL
            # CREATE TABLE name AS SELECT * FROM (VALUES ...) AS sub(col1, col2)
            create_sql = f"CREATE TABLE {name} AS SELECT * FROM (VALUES {values_clause}) AS sub({cols_def})"
            
            result = self._execute_dml(create_sql)
            
            # If there are more batches, insert them
            if batch_size and total_rows > batch_size:
                remaining_data = data.slice(batch_size)
                self.insert(name, data=remaining_data, batch_size=batch_size)
                
            return result

        sql = self._compile_sql()
        create_sql = f"CREATE TABLE {name} AS {sql}"
        return self._execute_dml(create_sql)

    def insert(self, table_name: str, data: Union[Any, None] = None, batch_size: Optional[int] = None):
        """
        Insert into table.
        If data is provided (Arrow Table or Pandas DataFrame), it generates a VALUES clause.
        Otherwise, it inserts from the current selection.
        
        Args:
            table_name: Target table name.
            data: Optional PyArrow Table or Pandas DataFrame.
            batch_size: Optional integer to split data into batches.
        """
        if data is not None:
            # Handle Arrow Table or Pandas DataFrame
            import pyarrow as pa
            import math
            
            if isinstance(data, pd.DataFrame):
                data = pa.Table.from_pandas(data)
            
            if not isinstance(data, pa.Table):
                raise ValueError("Data must be a PyArrow Table or Pandas DataFrame")
            
            # Convert to VALUES clause
            rows = data.to_pylist()
            total_rows = len(rows)
            
            if batch_size is None:
                batch_size = total_rows
            
            num_batches = math.ceil(total_rows / batch_size)
            results = []
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_rows = rows[start_idx:end_idx]
                
                values_list = []
                for row in batch_rows:
                    row_vals = []
                    for val in row.values():
                        if isinstance(val, str):
                            row_vals.append(f"'{val}'")
                        elif val is None:
                            row_vals.append("NULL")
                        else:
                            row_vals.append(str(val))
                    values_list.append(f"({', '.join(row_vals)})")
                
                values_clause = ", ".join(values_list)
                insert_sql = f"INSERT INTO {table_name} ({', '.join(data.column_names)}) VALUES {values_clause}"
                results.append(self._execute_dml(insert_sql))
                
            return results if len(results) > 1 else results[0]

        sql = self._compile_sql()
        insert_sql = f"INSERT INTO {table_name} {sql}"
        return self._execute_dml(insert_sql)

    def merge(self, target_table: str, on: Union[str, List[str]], 
              matched_update: Optional[Dict[str, str]] = None, 
              not_matched_insert: Optional[Dict[str, str]] = None,
              data: Union[Any, None] = None,
              batch_size: Optional[int] = None):
        """
        Perform a MERGE INTO operation (Upsert).
        
        Args:
            target_table: The table to merge into.
            on: Column(s) to join on.
            matched_update: Dict of {col: expr} to update when matched.
            not_matched_insert: Dict of {col: expr} to insert when not matched.
            data: Optional PyArrow Table or Pandas DataFrame to merge from.
            batch_size: Optional batch size for data.
        """
        if data is not None:
            # Similar to insert, handle data chunks
            import pyarrow as pa
            import math
            
            if isinstance(data, pd.DataFrame):
                data = pa.Table.from_pandas(data)
            
            rows = data.to_pylist()
            total_rows = len(rows)
            
            if batch_size is None:
                batch_size = total_rows
            
            num_batches = math.ceil(total_rows / batch_size)
            results = []
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_rows = rows[start_idx:end_idx]
                
                # Create a VALUES clause for the source
                values_list = []
                for row in batch_rows:
                    row_vals = []
                    for val in row.values():
                        if isinstance(val, str):
                            row_vals.append(f"'{val}'")
                        elif val is None:
                            row_vals.append("NULL")
                        else:
                            row_vals.append(str(val))
                    values_list.append(f"({', '.join(row_vals)})")
                
                values_clause = ", ".join(values_list)
                source_alias = "source"
                # Construct the source subquery
                # We need to cast columns if necessary, but for now assume simple values
                # Dremio VALUES syntax: (val1, val2), ...
                # But we need to alias columns: SELECT * FROM (VALUES ...) AS source(col1, col2)
                cols_def = ", ".join(data.column_names)
                source_sql = f"(VALUES {values_clause}) AS {source_alias}({cols_def})"
                
                results.append(self._compile_and_run_merge(target_table, source_sql, on, matched_update, not_matched_insert))
            
            return results if len(results) > 1 else results[0]
        else:
            # Use current builder as source
            source_sql = f"({self._compile_sql()}) AS source"
            return self._compile_and_run_merge(target_table, source_sql, on, matched_update, not_matched_insert)

    def _compile_and_run_merge(self, target_table, source_sql, on, matched_update, not_matched_insert):
        if isinstance(on, str):
            on = [on]
        
        on_clause = " AND ".join([f"{target_table}.{col} = source.{col}" for col in on])
        
        parts = [f"MERGE INTO {target_table} USING {source_sql} ON ({on_clause})"]
        
        if matched_update:
            updates = ", ".join([f"{col} = {expr}" for col, expr in matched_update.items()])
            parts.append(f"WHEN MATCHED THEN UPDATE SET {updates}")
            
        if not_matched_insert:
            cols = ", ".join(not_matched_insert.keys())
            vals = ", ".join(not_matched_insert.values())
            parts.append(f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})")
            
        merge_sql = " ".join(parts)
        return self._execute_dml(merge_sql)

    def delete(self):
        """Delete from table based on filters"""
        if not self.path:
            raise ValueError("Delete requires a table path defined in constructor")
        
        if not self.filters:
            raise ValueError("Delete requires at least one filter to prevent accidental data loss")

        where_clause = " AND ".join(self.filters)
        delete_sql = f"DELETE FROM {self.path} WHERE {where_clause}"
        return self._execute_dml(delete_sql)

    def update(self, updates: dict):
        """Update table based on filters"""
        if not self.path:
            raise ValueError("Update requires a table path defined in constructor")
        
        if not self.filters:
            raise ValueError("Update requires at least one filter")

        set_clause = ", ".join([f"{k} = {v}" for k, v in updates.items()])
        where_clause = " AND ".join(self.filters)
        update_sql = f"UPDATE {self.path} SET {set_clause} WHERE {where_clause}"
        return self._execute_dml(update_sql)

    def _execute_dml(self, sql: str):
        # DML can be executed via Flight or REST. Flight is usually for large result sets.
        # For DML, we might want to use the REST API SQL endpoint if it exists, or just Flight.
        # Flight is fine for DML too, it just returns a result with affected rows.
        return self._execute_flight(sql, "polars")
