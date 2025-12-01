# Function Reference

This document lists the SQL functions supported by `dremioframe.functions`.

## General Functions

- `col(name)`: Creates a column expression.
- `lit(val)`: Creates a literal expression.

## Aggregates

- `sum(col)`: Calculates the sum of a column.
- `avg(col)`: Calculates the average of a column.
- `min(col)`: Finds the minimum value in a column.
- `max(col)`: Finds the maximum value in a column.
- `count(col)`: Counts the number of non-null values in a column.
- `stddev(col)`: Calculates the standard deviation.
- `variance(col)`: Calculates the variance.
- `approx_distinct(col)`: Approximates the count of distinct values.

## Math

- `abs(col)`: Absolute value.
- `ceil(col)`: Ceiling.
- `floor(col)`: Floor.
- `round(col, scale=0)`: Rounds to the specified scale.
- `sqrt(col)`: Square root.
- `exp(col)`: Exponential.
- `ln(col)`: Natural logarithm.
- `log(base, col)`: Logarithm with specified base.
- `pow(col, power)`: Power.

## String

- `upper(col)`: Converts to uppercase.
- `lower(col)`: Converts to lowercase.
- `concat(*cols)`: Concatenates strings.
- `substr(col, start, length=None)`: Substring.
- `trim(col)`: Trims whitespace from both ends.
- `ltrim(col)`: Trims whitespace from left.
- `rtrim(col)`: Trims whitespace from right.
- `length(col)`: String length.
- `replace(col, pattern, replacement)`: Replaces occurrences of pattern.
- `regexp_replace(col, pattern, replacement)`: Replaces using regex.
- `initcap(col)`: Capitalizes first letter of each word.

## Date/Time

- `current_date()`: Current date.
- `current_timestamp()`: Current timestamp.
- `date_add(col, days)`: Adds days to date.
- `date_sub(col, days)`: Subtracts days from date.
- `date_diff(col1, col2)`: Difference in days between dates.
- `to_date(col, fmt=None)`: Converts string to date.
- `to_timestamp(col, fmt=None)`: Converts string to timestamp.
- `year(col)`: Extracts year.
- `month(col)`: Extracts month.
- `day(col)`: Extracts day.
- `hour(col)`: Extracts hour.
- `minute(col)`: Extracts minute.
- `second(col)`: Extracts second.
- `extract(field, source)`: Extracts field from source.

## Conditional

- `coalesce(*cols)`: Returns first non-null value.
- `when(condition, value)`: Starts a CASE statement builder.

## Window Functions

- `rank()`: Rank.
- `dense_rank()`: Dense rank.
- `row_number()`: Row number.
- `lead(col, offset=1, default=None)`: Lead.
- `lag(col, offset=1, default=None)`: Lag.
- `first_value(col)`: First value in window.
- `last_value(col)`: Last value in window.
- `ntile(n)`: N-tile.

## AI Functions

- `ai_classify(prompt, categories, model_name=None)`: Classifies text into categories.
- `ai_complete(prompt, model_name=None)`: Generates text completion.
- `ai_generate(prompt, model_name=None, schema=None)`: Generates structured data.

## Complex Types

- `flatten(col)`: Explodes a list into multiple rows.
- `convert_from(col, type_)`: Converts from serialized format.
- `convert_to(col, type_)`: Converts to serialized format.
