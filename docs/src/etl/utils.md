# Utils Module

Utility functions used across the ETL pipeline.

## Functions
- `clean_df(df)` – Drops list/struct columns and casts Boolean columns to `Int8` for Oracle compatibility.
- `id_generator()` – Generates unique request IDs.

## Example
```python
from etl.utils import clean_df, id_generator
request_id = id_generator()
cleaned = clean_df(df)
```

## Source
::: etl.utils