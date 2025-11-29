import pytest
from dremioframe import F

def test_flatten():
    expr = F.flatten("items")
    assert str(expr) == "FLATTEN(items)"

def test_convert_from():
    expr = F.convert_from("json_col", "JSON")
    assert str(expr) == "CONVERT_FROM(json_col, 'JSON')"

def test_convert_to():
    expr = F.convert_to("map_col", "JSON")
    assert str(expr) == "CONVERT_TO(map_col, 'JSON')"
