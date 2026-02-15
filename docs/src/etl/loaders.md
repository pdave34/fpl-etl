# Loaders Module

This module handles writing transformed data to storage formats such as Parquet files or generating Oracle DDL statements.

## Key Functions
- `write_parquet(df, filename)` – Write a Polars `DataFrame` to a Parquet file.
- `generate_oracle_ddl(df, table_name)` – Produce DDL for Oracle based on the DataFrame schema.

## Usage
```python
from etl.loaders import write_parquet
write_parquet(df, "data/output.parquet")
```

## Source
::: etl.loaders
