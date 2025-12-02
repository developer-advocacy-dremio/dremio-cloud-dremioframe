import pytest
from dremioframe.templates import QueryTemplate, TemplateLibrary, library

def test_query_template_render():
    """Test template rendering"""
    template = QueryTemplate("SELECT * FROM $table WHERE id = $id")
    sql = template.render(table="users", id=1)
    assert sql == "SELECT * FROM users WHERE id = 1"

def test_template_library_register_get():
    """Test library registration and retrieval"""
    lib = TemplateLibrary()
    lib.register("test", "SELECT 1")
    
    template = lib.get("test")
    assert isinstance(template, QueryTemplate)
    assert template.sql == "SELECT 1"
    
    with pytest.raises(KeyError):
        lib.get("nonexistent")

def test_template_library_render():
    """Test library rendering shortcut"""
    lib = TemplateLibrary()
    lib.register("hello", "SELECT '$msg'")
    
    sql = lib.render("hello", msg="world")
    assert sql == "SELECT 'world'"

def test_global_library_defaults():
    """Test default templates in global library"""
    assert library.get("row_count") is not None
    assert library.get("sample") is not None
    
    sql = library.render("row_count", table="my_table")
    assert sql == "SELECT COUNT(*) as count FROM my_table"
