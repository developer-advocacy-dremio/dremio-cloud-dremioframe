import pytest
from dremioframe.functions import *
from dremioframe.functions import Expr, Window

def test_expr_operators():
    c = col("a")
    assert str(c + 1) == "(a + 1)"
    assert str(c - 1) == "(a - 1)"
    assert str(c * 2) == "(a * 2)"
    assert str(c / 2) == "(a / 2)"
    assert str(c == 1) == "(a = 1)"
    assert str(c > 1) == "(a > 1)"
    assert str(c & (col("b") == 2)) == "(a AND (b = 2))"
    assert str(~c) == "(NOT a)"

def test_expr_methods():
    c = col("a")
    assert str(c.alias("b")) == "a AS b"
    assert str(c.cast("INT")) == "CAST(a AS INT)"
    assert str(c.is_null()) == "(a IS NULL)"
    assert str(c.isin([1, 2])) == "(a IN (1, 2))"

def test_aggregates():
    assert str(sum("a")) == "SUM(a)"
    assert str(avg(col("a"))) == "AVG(a)"
    assert str(count("*")) == "COUNT(*)"

def test_math():
    assert str(abs(col("x"))) == "ABS(x)"
    assert str(round(col("x"), 2)) == "ROUND(x, 2)"

def test_string():
    assert str(upper(col("s"))) == "UPPER(s)"
    assert str(concat("a", "b")) == "CONCAT(a, b)"
    assert str(substr("s", 1, 3)) == "SUBSTR(s, 1, 3)"

def test_date():
    assert str(year("d")) == "YEAR(d)"
    assert str(date_add("d", 1)) == "DATE_ADD(d, 1)"

def test_case_when():
    expr = when("x > 1", "A").when("x < 0", "B").otherwise("C")
    assert str(expr) == "CASE WHEN x > 1 THEN A WHEN x < 0 THEN B ELSE C END"

def test_window():
    w = Window.partition_by("dept").order_by("salary")
    assert str(rank().over(w)) == "RANK() OVER (PARTITION BY dept ORDER BY salary)"
    
    w2 = Window.order_by("time").rows_between("UNBOUNDED PRECEDING", "CURRENT ROW")
    assert str(sum("val").over(w2)) == "SUM(val) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

def test_ai_functions():
    # AI_CLASSIFY
    assert str(ai_classify("Is this spam?", ["Yes", "No"])) == "AI_CLASSIFY('Is this spam?', ARRAY['Yes', 'No'])"
    assert str(ai_classify("Spam?", ["Yes", "No"], "gpt4")) == "AI_CLASSIFY('gpt4', 'Spam?', ARRAY['Yes', 'No'])"
    
    # AI_COMPLETE
    assert str(ai_complete("Write a poem")) == "AI_COMPLETE('Write a poem')"
    assert str(ai_complete("Write a poem", "gpt4")) == "AI_COMPLETE('gpt4', 'Write a poem')"
    
    # AI_GENERATE
    assert str(ai_generate("Extract info")) == "AI_GENERATE('Extract info')"
    assert str(ai_generate("Extract info", "gpt4")) == "AI_GENERATE('gpt4', 'Extract info')"
    assert str(ai_generate("Extract info", schema="ROW(name VARCHAR)")) == "AI_GENERATE('Extract info' WITH SCHEMA ROW(name VARCHAR))"
    assert str(ai_generate("Extract info", "gpt4", "ROW(name VARCHAR)")) == "AI_GENERATE('gpt4', 'Extract info' WITH SCHEMA ROW(name VARCHAR))"
