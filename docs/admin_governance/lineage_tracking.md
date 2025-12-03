# Data Lineage Tracking

DremioFrame provides a `LineageTracker` to track, visualize, and export data lineage for governance, impact analysis, and documentation.

## LineageTracker

The `LineageTracker` maintains a graph of data transformations showing how data flows through your pipelines.

### Initialization

```python
from dremioframe.client import DremioClient
from dremioframe.lineage import LineageTracker

client = DremioClient()
tracker = LineageTracker(client)
```

### Tracking Transformations

Record data transformations as they occur:

```python
# Track an INSERT operation
tracker.track_transformation(
    source="raw.events",
    target="staging.events",
    operation="insert",
    metadata={"rows": 10000, "timestamp": "2024-01-01"}
)

# Track a SELECT transformation
tracker.track_transformation(
    source="staging.events",
    target="analytics.daily_summary",
    operation="aggregate",
    metadata={"group_by": "date"}
)

# Track a JOIN
tracker.track_transformation(
    source="staging.events",
    target="analytics.enriched_events",
    operation="join",
    metadata={"join_table": "dim.customers"}
)
```

### Querying Lineage

Get upstream or downstream dependencies:

```python
# Get all upstream sources for a table
lineage = tracker.get_lineage_graph(
    table="analytics.daily_summary",
    direction="upstream",
    max_depth=5
)

print(f"Upstream dependencies: {list(lineage.nodes.keys())}")
# Output: ['staging.events', 'raw.events']

# Get all downstream consumers
lineage = tracker.get_lineage_graph(
    table="raw.events",
    direction="downstream"
)

print(f"Downstream consumers: {list(lineage.nodes.keys())}")
# Output: ['staging.events', 'analytics.daily_summary', 'analytics.enriched_events']
```

### Visualizing Lineage

Create interactive HTML visualizations:

```python
# Visualize the entire lineage graph
html = tracker.visualize(format='html', output_file='lineage.html')

# Visualize lineage for a specific table
lineage = tracker.get_lineage_graph("analytics.daily_summary", direction="both")
tracker.visualize(lineage, format='html', output_file='daily_summary_lineage.html')
```

The HTML visualization uses [vis.js](https://visjs.org/) to create an interactive, draggable graph.

### Static Visualizations

Create static images using Graphviz (requires optional dependency):

```python
# Requires: pip install dremioframe[lineage]

# Generate PNG
tracker.visualize(format='png', output_file='lineage.png')

# Generate SVG
tracker.visualize(format='svg', output_file='lineage.svg')
```

### Exporting Lineage

Export lineage data for integration with external tools:

```python
# Export to JSON
json_data = tracker.export_lineage(format='json', output_file='lineage.json')

# Export to DataHub format
datahub_data = tracker.export_lineage(format='datahub')

# Export to Amundsen format
amundsen_data = tracker.export_lineage(format='amundsen')
```

## Use Cases

### 1. Impact Analysis

Understand what will be affected by schema changes:

```python
# Before modifying raw.events, check downstream impact
lineage = tracker.get_lineage_graph("raw.events", direction="downstream")

print(f"Tables affected by changes to raw.events:")
for node_id in lineage.nodes:
    print(f"  - {node_id}")
```

### 2. Data Governance

Document data flows for compliance:

```python
# Track all transformations in your pipeline
def run_etl_pipeline():
    # Extract
    tracker.track_transformation("source.db.customers", "raw.customers", "extract")
    
    # Transform
    tracker.track_transformation("raw.customers", "staging.customers", "clean")
    tracker.track_transformation("staging.customers", "analytics.customers", "aggregate")
    
    # Export lineage for audit
    tracker.export_lineage(format='json', output_file='audit/lineage_2024_01.json')
```

### 3. Pipeline Documentation

Auto-generate pipeline documentation:

```python
# Create visual documentation
for table in ["analytics.sales", "analytics.customers", "analytics.products"]:
    lineage = tracker.get_lineage_graph(table, direction="both")
    tracker.visualize(lineage, format='html', 
                     output_file=f'docs/{table.replace(".", "_")}_lineage.html')
```

### 4. Root Cause Analysis

Trace data quality issues to their source:

```python
# If analytics.daily_summary has bad data, trace it back
lineage = tracker.get_lineage_graph("analytics.daily_summary", direction="upstream")

print("Data sources to investigate:")
for node_id in lineage.nodes:
    node = lineage.nodes[node_id]
    if node.type == 'table':
        print(f"  - {node_id}")
```

## LineageGraph API

The `LineageGraph` object provides programmatic access to lineage data:

```python
lineage = tracker.get_lineage_graph("my.table")

# Access nodes
for node in lineage.nodes.values():
    print(f"{node.name} ({node.type})")

# Access edges
for edge in lineage.edges:
    print(f"{edge.source_id} --[{edge.operation}]--> {edge.target_id}")

# Convert to NetworkX for advanced analysis
nx_graph = lineage.to_networkx()  # Requires networkx

# Export to dict
data = lineage.to_dict()
```

## Integration with External Tools

### DataHub

```python
# Export lineage for DataHub ingestion
datahub_json = tracker.export_lineage(format='datahub')

# Use DataHub's REST API or CLI to ingest
# datahub ingest -c lineage_config.yml
```

### Amundsen

```python
# Export for Amundsen
amundsen_json = tracker.export_lineage(format='amundsen')

# Load into Amundsen's metadata service
```

## Best Practices

1. **Track at Key Points**: Record transformations at major pipeline stages (extract, transform, load)
2. **Include Metadata**: Add context like row counts, timestamps, and transformation logic
3. **Regular Exports**: Periodically export lineage for backup and audit
4. **Visualize Often**: Use visualizations to communicate with stakeholders
5. **Combine with DQ**: Link lineage with data quality checks for comprehensive governance

## Limitations

- **Manual Tracking**: Transformations must be explicitly tracked (not auto-detected)
- **In-Memory**: Lineage graph is stored in memory (export for persistence)
- **No Version History**: Current implementation doesn't track lineage changes over time
