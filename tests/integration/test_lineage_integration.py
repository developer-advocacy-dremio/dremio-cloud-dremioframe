import pytest
from dremioframe.client import DremioClient
from dremioframe.lineage import LineageTracker
import tempfile
import os

@pytest.fixture
def client():
    """Create a real Dremio client for integration testing"""
    return DremioClient()

def test_track_real_transformation(client):
    """Test tracking transformations in a real pipeline"""
    tracker = LineageTracker(client)
    
    # Track a simple transformation pipeline
    tracker.track_transformation(
        source="sys.version",
        target="test.version_copy",
        operation="select",
        metadata={"description": "Copy of system version table"}
    )
    
    # Verify tracking
    assert len(tracker.graph.nodes) == 2
    assert len(tracker.graph.edges) == 1
    
    # Get lineage
    lineage = tracker.get_lineage_graph("test.version_copy", direction="upstream")
    assert "sys.version" in lineage.nodes
    
    print(f"\\nTracked {len(tracker.graph.nodes)} nodes and {len(tracker.graph.edges)} edges")

def test_visualize_lineage_html(client):
    """Test HTML visualization generation"""
    tracker = LineageTracker(client)
    
    # Create a simple lineage
    tracker.track_transformation("source.table1", "staging.table1", "insert")
    tracker.track_transformation("staging.table1", "analytics.table1", "select")
    
    # Generate HTML
    html = tracker.visualize(format='html')
    
    assert '<html>' in html.lower()
    assert 'source.table1' in html
    assert 'analytics.table1' in html
    
    # Save to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(html)
        temp_path = f.name
    
    print(f"\\nVisualization saved to: {temp_path}")
    
    # Verify file was created
    assert os.path.exists(temp_path)
    
    # Cleanup
    os.unlink(temp_path)

def test_export_lineage_json(client):
    """Test JSON export"""
    tracker = LineageTracker(client)
    
    tracker.track_transformation("raw.events", "staging.events", "insert")
    tracker.track_transformation("staging.events", "analytics.events", "aggregate")
    
    json_export = tracker.export_lineage(format='json')
    
    assert 'nodes' in json_export
    assert 'edges' in json_export
    assert 'raw.events' in json_export
    
    print(f"\\nExported lineage:\\n{json_export[:200]}...")
