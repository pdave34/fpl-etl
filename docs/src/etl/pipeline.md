# Pipeline Module

The pipeline orchestrates the extract‑transform‑load process.

## Function
- `run_pipeline()` – Executes the full ETL flow: extracts data, cleans it, and loads it.

## Example
```python
from etl.pipeline import run_pipeline
run_pipeline()
```

## Source
::: etl.pipeline
