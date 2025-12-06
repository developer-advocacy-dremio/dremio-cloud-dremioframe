import pytest
from unittest.mock import MagicMock, patch
import os
import json

# Mock fastmcp to avoid import errors during test collection if dependencies missing

def test_read_library_doc():
    from dremioframe.ai.server import read_library_doc
    
    # Create a dummy doc file
    os.makedirs("docs/test_cat", exist_ok=True)
    with open("docs/test_cat/test.md", "w") as f:
        f.write("# Test Doc")
        
    try:
        content = read_library_doc("test_cat", "test.md")
        assert content == "# Test Doc"
    finally:
        if os.path.exists("docs/test_cat/test.md"):
            os.remove("docs/test_cat/test.md")
        if os.path.exists("docs/test_cat"):
            os.rmdir("docs/test_cat")

def test_list_available_docs():
    from dremioframe.ai.server import list_available_docs
    
    # Create dummy docs
    os.makedirs("docs/test_cat", exist_ok=True)
    with open("docs/test_cat/test.md", "w") as f:
        f.write("# Test Doc")
        
    try:
        docs = list_available_docs()
        assert "Library: test_cat/test.md" in docs
    finally:
        if os.path.exists("docs/test_cat/test.md"):
            os.remove("docs/test_cat/test.md")
        if os.path.exists("docs/test_cat"):
            os.rmdir("docs/test_cat")
