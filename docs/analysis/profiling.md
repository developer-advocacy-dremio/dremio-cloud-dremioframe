# Query Profile Analyzer

Analyze and visualize Dremio query execution profiles.

## Usage

### Get Job Profile

```python
profile = client.admin.get_job_profile("job_id_123")
```

### Summary

Print a summary of the job execution.

```python
profile.summary()
# Job ID: job_id_123
# State: COMPLETED
# Start: 1600000000000
# End: 1600000005000
```

### Visualize

Visualize the execution timeline using Plotly.

```python
# Display interactive chart
profile.visualize().show()

# Save to HTML
profile.visualize(save_to="profile.html")
```
