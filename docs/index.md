# FPL ETL Documentation

Welcome to the FPL ETL project documentation. This project provides a lightweight ETL pipeline that extracts data from external APIs (e.g., Fantasy Premier League), cleans it, and passes PyArrow tables for downstream analytics.

## Overview
- **Extractors**: Pull data from APIs (e.g., Fantasy Premier League) and return Polars DataFrames.
- **Utils**: Helper functions for data cleaning and ID generation.
- **Loaders**: Write data to Parquet or generate Oracle DDL.
- **Pipeline**: Orchestrates the extract‑transform‑load flow.

See the navigation menu on the left for detailed module documentation.