# Hospital Price Transparency — Data Pipeline & Analysis

This project processes and analyzes hospital price transparency files under the Hospital Price Transparency Act. The goal is to **unify heterogeneous data formats** into a consistent schema and perform exploratory and comparative analysis of pricing variation across hospitals, payers, and procedures.

## File Overview

- **data/**  
  Raw input files (CSV, JSON, XLSX, etc.) downloaded from hospital transparency portals.

- **processed/**  
  Standardized outputs after schema unification (e.g., `merged_standardized.csv`, parquet files).

- **unify_schema.py**  
  Defines the **standard schema** (columns like `hospital`, `payer_name`, `plan_name`, `gross_charge`, `discounted_cash`, `negotiated_dollar`, `code`, `code_system`) and provides functions to normalize different hospital file formats.

- **run_unify.py**  
  Main entry point to run the schema unification across multiple hospitals. Calls `unify_schema.py` on raw files and outputs standardized CSV/Parquet.

- **save_parquet.py**  
  Utility script to save processed datasets into efficient Parquet format for downstream analysis.

- **aggregate_prices.py**  
  Aggregates processed data (e.g., by hospital, payer, or code) to generate summary statistics for analysis.

- **eda_basic.py**  
  Performs **exploratory data analysis** (EDA): summary stats, distributions of gross vs. discounted prices, basic variation measures.

- **analysis_visualize.ipynb**  
  Interactive Jupyter notebook with visualizations:  
  - Distribution of charges (gross vs. discounted)  
  - Discount ratios across hospitals  
  - Variation measures (CV, IQR, Gini)  
  - Cross-hospital procedure comparisons  
  - Payer-specific benchmarking

- **readme.txt**  
  (This file) High-level project documentation.