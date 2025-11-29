import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from dremioframe.builder import DremioBuilder

@pytest.fixture
def mock_builder():
    builder = DremioBuilder(MagicMock(), "source.table")
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "cat": ["a", "b", "a"]})
    builder.collect = MagicMock(return_value=df)
    return builder

def test_chart_matplotlib_default(mock_builder):
    with patch("matplotlib.pyplot.savefig") as mock_save:
        # Mock pandas plot
        with patch("pandas.DataFrame.plot") as mock_plot:
            mock_builder.chart(kind="line", x="x", y="y")
            mock_plot.assert_called_with(kind="line", x="x", y="y", title=None)

def test_chart_plotly(mock_builder):
    with patch("plotly.express.line") as mock_line:
        mock_fig = MagicMock()
        mock_line.return_value = mock_fig
        
        mock_builder.chart(kind="line", x="x", y="y", backend="plotly")
        
        mock_line.assert_called()
        # Verify arguments passed to px.line
        call_args = mock_line.call_args
        assert call_args[1]["x"] == "x"
        assert call_args[1]["y"] == "y"

def test_chart_plotly_save_html(mock_builder):
    with patch("plotly.express.bar") as mock_bar:
        mock_fig = MagicMock()
        mock_bar.return_value = mock_fig
        
        mock_builder.chart(kind="bar", x="x", y="y", backend="plotly", save_to="chart.html")
        
        mock_fig.write_html.assert_called_with("chart.html")

def test_chart_plotly_unsupported_kind(mock_builder):
    with pytest.raises(ValueError):
        mock_builder.chart(kind="hexbin", backend="plotly")
