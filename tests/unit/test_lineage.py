import pytest
from dremioframe.lineage import LineageTracker, LineageGraph, LineageNode, LineageEdge
import json

def test_lineage_graph_creation():
    """Test creating a lineage graph"""
    graph = LineageGraph()
    
    node1 = LineageNode(id="table1", name="Table 1", type="table")
    node2 = LineageNode(id="table2", name="Table 2", type="view")
    
    graph.add_node(node1)
    graph.add_node(node2)
    
    assert len(graph.nodes) == 2
    assert "table1" in graph.nodes
    assert "table2" in graph.nodes

def test_lineage_graph_edges():
    """Test adding edges to graph"""
    graph = LineageGraph()
    
    graph.add_node(LineageNode(id="source", name="Source", type="table"))
    graph.add_node(LineageNode(id="target", name="Target", type="table"))
    
    edge = LineageEdge(source_id="source", target_id="target", operation="select")
    graph.add_edge(edge)
    
    assert len(graph.edges) == 1
    assert graph.edges[0].source_id == "source"
    assert graph.edges[0].target_id == "target"

def test_get_upstream():
    """Test getting upstream dependencies"""
    graph = LineageGraph()
    
    # Create chain: A -> B -> C
    for node_id in ["A", "B", "C"]:
        graph.add_node(LineageNode(id=node_id, name=node_id, type="table"))
    
    graph.add_edge(LineageEdge(source_id="A", target_id="B", operation="select"))
    graph.add_edge(LineageEdge(source_id="B", target_id="C", operation="select"))
    
    upstream_c = graph.get_upstream("C")
    assert "B" in upstream_c
    assert "A" in upstream_c
    
    upstream_b = graph.get_upstream("B")
    assert "A" in upstream_b
    assert "C" not in upstream_b

def test_get_downstream():
    """Test getting downstream dependencies"""
    graph = LineageGraph()
    
    # Create chain: A -> B -> C
    for node_id in ["A", "B", "C"]:
        graph.add_node(LineageNode(id=node_id, name=node_id, type="table"))
    
    graph.add_edge(LineageEdge(source_id="A", target_id="B", operation="select"))
    graph.add_edge(LineageEdge(source_id="B", target_id="C", operation="select"))
    
    downstream_a = graph.get_downstream("A")
    assert "B" in downstream_a
    assert "C" in downstream_a
    
    downstream_b = graph.get_downstream("B")
    assert "C" in downstream_b
    assert "A" not in downstream_b

def test_track_transformation():
    """Test tracking a transformation"""
    tracker = LineageTracker()
    
    tracker.track_transformation(
        source="raw.events",
        target="staging.events",
        operation="insert",
        metadata={"rows": 1000}
    )
    
    assert len(tracker.graph.nodes) == 2
    assert len(tracker.graph.edges) == 1
    assert "raw.events" in tracker.graph.nodes
    assert "staging.events" in tracker.graph.nodes

def test_get_lineage_graph_upstream():
    """Test getting upstream lineage graph"""
    tracker = LineageTracker()
    
    # Create lineage: raw -> staging -> analytics
    tracker.track_transformation("raw.data", "staging.data", "insert")
    tracker.track_transformation("staging.data", "analytics.data", "select")
    
    lineage = tracker.get_lineage_graph("analytics.data", direction="upstream")
    
    assert "analytics.data" in lineage.nodes
    assert "staging.data" in lineage.nodes
    assert "raw.data" in lineage.nodes

def test_get_lineage_graph_downstream():
    """Test getting downstream lineage graph"""
    tracker = LineageTracker()
    
    tracker.track_transformation("raw.data", "staging.data", "insert")
    tracker.track_transformation("staging.data", "analytics.data", "select")
    
    lineage = tracker.get_lineage_graph("raw.data", direction="downstream")
    
    assert "raw.data" in lineage.nodes
    assert "staging.data" in lineage.nodes
    assert "analytics.data" in lineage.nodes

def test_graph_to_dict():
    """Test exporting graph to dictionary"""
    graph = LineageGraph()
    
    graph.add_node(LineageNode(id="table1", name="Table 1", type="table"))
    graph.add_node(LineageNode(id="table2", name="Table 2", type="view"))
    graph.add_edge(LineageEdge(source_id="table1", target_id="table2", operation="select"))
    
    data = graph.to_dict()
    
    assert 'nodes' in data
    assert 'edges' in data
    assert len(data['nodes']) == 2
    assert len(data['edges']) == 1

def test_export_lineage_json():
    """Test exporting lineage to JSON"""
    tracker = LineageTracker()
    
    tracker.track_transformation("source", "target", "insert")
    
    json_data = tracker.export_lineage(format='json')
    
    parsed = json.loads(json_data)
    assert 'nodes' in parsed
    assert 'edges' in parsed

def test_visualize_html():
    """Test HTML visualization generation"""
    tracker = LineageTracker()
    
    tracker.track_transformation("source", "target", "insert")
    
    html = tracker.visualize(format='html')
    
    assert '<html>' in html.lower()
    assert 'vis-network' in html.lower()
    assert 'source' in html
    assert 'target' in html
