# Data Curation — Project Documentation

## Overview

This repository contains code and scripts to build a curated dataset that combines bicycle sharing trip data from two cities (Washington, D.C. and Los Angeles) with contextual weather and demographic information. The curated outputs are intended for reproducible analysis (descriptive & predictive).

This documentation covers:
- Project purpose and scope
- Workflow (acquisition → cleaning & homogenization → feature engineering)
- Data dictionaries & output files
- Dependencies and recommended environment
- How-to run the full pipeline and expected outputs
- Notebooks for analysis (descriptive & predictive)
- Metadata references

---

## Purpose & Use Cases

This curated dataset is useful for:
- Analyzing usage and temporal patterns (e.g., usage by hour, day, season)
- Studying weather impacts and socio-demographic patterns in bike-share trips
- Building and evaluating predictive model of trip duration
- Demonstrating reproducible data curation workflows

---

## Repository structure

Root
- `RawData/` — raw provider data from MetroBike, CaptialBike and Open-Meteo CSVs 
- `TempData/` — intermediate outputs created by the pipeline (not tracked or included by default)
- `CuratedData/` — curated outputs (final data, data dictionaries, metadata, and provenance)
- `SourceCode/` — core modules for acquisition, cleaning, feature engineering, and preparing data for predictive analysis
- `analysis_descriptive.ipynb` — descriptive analysis notebook
- `analysis_predictive.ipynb` — predictive analysis notebook

Curated structure (`CuratedData/`)
- `stations.csv` — station table
- `bikes.csv` — bikes table
- `demographics.csv` — zip code demographic information
- `weather.csv` — consolidated weather (daily/hours) for Washington D.C. and Los Angeles cities
- `trips_{YYYY}_{MM}.csv` — homogenized trips table by month
- `features_{YYYY}_{MM}.csv` — per-trip features suitable for modeling (joined with stations)
- `Documentation/DataDictionaries/` — JSON dictionaries describing schema of each curated CSV
- `Documentation/metadata_datacite.json` — DataCite metadata for the dataset

---

## Workflow summary

The pipeline is expressed in `SourceCode/DataCuration/run_pipeline.py` and includes the following high-level steps:

1. Data Acquisition
    - Manual download of zip files containing Metro and Capital Bikeshare datasets (included in the repository)
    - Unzips archive files for Metro and Capital Bikeshare
    - Downloads weather via Open-Meteo for the cities
    - Contain the functions to  fetch demographic data via U.S. Census ACS but they are use later when all the zip codes are obtained

2. Data Cleaning and Homogenization
    - Normalize schema to a canonical form (common keys, datatypes, fields)
    - Clean station and bike information, eliminate duplicate/invalid rows
    - Clean trips and create UUIDs as primary and foreign keys to join tables 
    - Output: `trips_YYYY_MM.csv` files in `CuratedData` and `stations.csv`, `bikes.csv`, `demographics.csv`

3. Feature Engineering
    - Combine information with stations to get geolocation to calculate distances
    - Transform trips into modeling features (time and distance derived features)
    - Save datasets with feature engineering in format `features_YYYY_MM.csv`

4. Analysis
    - Use `analysis_descriptive.ipynb` for exploratory plots and usage patterns
    - Use `analysis_predictive.ipynb` for training and evaluation of model to predict trip duration

Please review `workflow_example.ipynb` to have a deeper understanding of the main steps in the data curation workflow.

---

## How to run the pipeline

Recommended: create an isolated virtual environment, install the required dependencies listed in `requirements.txt`, using Python version 3.11.4 in a Windows Operating System.

Windows PowerShell

```powershell
# From repository root
python -m venv .venv
# Activate the venv in PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Optionally set your Census API key in the current session
$env:US_CENSUS_API_KEY = "YOUR_KEY"

# Run the coordinated pipeline (powershell launcher makes things easier in Windows)
.
\SourceCode\DataCuration\automated_workflow.ps1

# Or run the python run_pipeline orchestrator directly
python -m SourceCode.DataCuration.run_pipeline --raw-data .\RawData --temp-data .\TempData --curated-data .\CuratedData
```

Linux / macOS (bash)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m SourceCode.DataCuration.run_pipeline --raw-data ./RawData --temp-data ./TempData --curated-data ./CuratedData
```

Flags of interest
- `--no-unzip` — skip unzipping archives if raw files are already extracted
- `--skip-demographics` — skip Census calls if you don't have `US_CENSUS_API_KEY`
- `--skip-weather` — skip Open-Meteo weather gathering
- `--features-all` — create features for all monthly trip files instead of a subset

---

## Expected outputs

After running the pipeline you should see the following in `CuratedData/`:
- `stations.csv` — station catalog (UUIDs)
- `bikes.csv` — bike catalog
- `trips_YYYY_MM.csv` — homogenized monthly trips (one file per month)
- `features_YYYY_MM.csv` — per-trip features suitable for analysis and modeling
- `weather.csv` — daily or hourly weather data for cities
- `demographics.csv` — zip-level census-derived variables used in the models


Note: files are typically generated by `run_pipeline.py` and saved to `CuratedData/`. Intermediate files may live in `TempData/`.

---

## Data dictionaries

Schema files in `CuratedData/Documentation/DataDictionaries/` describe each CSV's schema, including data types, and descriptions for each field. These files are intended for users and to enable discoverability and reproducibility.

---

## Analysis notebooks

- `analysis_descriptive.ipynb` — step-by-step exploratory data analysis using `features_YYYY_MM.csv` files to visualize usage patterns, weather effects, socio-demographic insights, and station level demand summary.
- `analysis_predictive.ipynb` — demonstrates training a regression model (XGBoost) to predict trip duration using per-trip features and insights into model performance and feature importance.

How to run the notebooks
- Use your preferred Jupyter interface (e.g., `jupyter lab`, `notebook` or VS Code) inside the activated virtual environment.
- Make sure `CuratedData/` contains the outputs from completing the data curation pipelin to run the notebooks.

---

## Metadata

- The dataset-level DataCite metadata is provided in `CuratedData/metadata_datacite.json`.

Please also refer to the data dictionaries provided in `CuratedData/Documentation/DataDictionaries/`, `README.md` and this file `DATA_CURATION.md` for more information

---

## Limitations & Notes

- Data quality: The pipeline attempts to discard anomalous trips (unrealistic speed) — check `feature_engineering.py` for thresholds.
- Scope: Only Washington D.C. (Capital Bikeshare) and Los Angeles (Metro) are currently implemented.
- Census calls: `uszipcode` or US Census requests are used to fetch demographics; running without an API key will disable ACS calls.

---

## Contact & License

For questions, please use the repo issue tracker or contact the repository owner.

The data in this repository comes from public sources; check the data providers' license terms. Clarify licensing before reuse.

---

## How to add more cities

To expand to additional cities, add: (1) zip/csv acquisition functions in `RawData/` and data acquisition scripts, (2) extend the `data_cleaning_homogenization` function to include vendor-specific logic and (3) update stations mapping and DEM/Census steps to include the new city's lat/lon census linkage.

---

## Changes history

- 2025-12-02: New documentation and metadata added (this file)

