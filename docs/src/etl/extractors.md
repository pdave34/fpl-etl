# Extractors Module

This module contains classes for extracting data from external APIs and converting them into Polars `DataFrame`s.

## Classes
- `APIExtractor` – Base class providing common extraction logic.
- `FPL` – Subclass for the Fantasy Premier League API.

## Usage
```python
from etl.extractors import FPL
extractor = FPL()
for name, df in extractor.generate():
    # Process DataFrame
    pass
```

## Source
::: etl.extractors
