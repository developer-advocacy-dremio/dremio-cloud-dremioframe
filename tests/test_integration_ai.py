import pytest
import os
from dremioframe.client import DremioClient

@pytest.mark.integration
def test_ai_classify_integration():
    # Assume environment variables are set for Dremio connection
    # If not, this test might need to be skipped or configured differently
    
    # Check if necessary env vars are present, otherwise skip
    url = os.getenv("DREMIO_URL")
    pat = os.getenv("DREMIO_PAT")
    project_id = os.getenv("DREMIO_PROJECT_ID")

    if not (url and pat and project_id):
        pytest.skip("Dremio credentials not found in environment variables")

    client = DremioClient(
        hostname=url,
        pat=pat,
        project_id=project_id
    )
    
    # The user provided query:
    # SELECT id,
    #        name,
    #        ingredients,
    #        AI_CLASSIFY('Identify the Spice Level:' || ARRAY_TO_STRING(ingredients, ','), ARRAY [ 'mild', 'medium', 'spicy' ]) AS spice_level
    # from   dremio.recipes.recipes

    builder = client.table("dremio.recipes.recipes")
    
    # We use mutate to add the spice_level column.
    # Note: The user wants to select id, name, ingredients AND the new column.
    # With the new logic, we can just select them and mutate spice_level.
    
    df = builder.select("id", "name", "ingredients") \
        .mutate(spice_level="AI_CLASSIFY('Identify the Spice Level:' || ARRAY_TO_STRING(ingredients, ','), ARRAY [ 'mild', 'medium', 'spicy' ])") \
        .collect()
        
    # Verify the result
    assert "spice_level" in df.columns
    assert "id" in df.columns
    assert "name" in df.columns
    assert "ingredients" in df.columns
    
    # Optional: Print first few rows to verify content visually if run with -s
    print(df.head())
