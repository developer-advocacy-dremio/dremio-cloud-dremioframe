# Interactive Plotting with Plotly

DremioFrame supports interactive charts using [Plotly](https://plotly.com/python/).

## Usage

Specify `backend="plotly"` in the `chart()` method.

```python
# Create an interactive scatter plot
fig = df.chart(
    kind="scatter",
    x="gdpPercap",
    y="lifeExp",
    color="continent",
    size="pop",
    hover_name="country",
    log_x=True,
    title="Life Expectancy vs GDP",
    backend="plotly"
)

# Display in notebook
fig.show()

# Save to HTML
df.chart(..., backend="plotly", save_to="chart.html")
```

## Supported Chart Types

- `line`
- `bar`
- `scatter`
- `pie`
- `histogram`
- `box`
- `violin`
- `area`

## Dependencies

Requires `plotly` and `pandas`.
For static image export (e.g., `.png`), `kaleido` is required.
