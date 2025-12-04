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
    - Use `analysis_descriptive.ipynb` for exploratory plots and usage patterns, weather effects, socio-demographic utilization, trip duration by user type, and station-level demand summary
    - Use `analysis_predictive.ipynb` for training and evaluation of model to predict trip duration based on time of day, weather, user type, bike type, zip code demographics. Finally, compare predictive performance across cities and the most important features to predict trip duration.


Please review `workflow_example.ipynb` to have a deeper understanding of the main steps in the data curation workflow.

---

## How to run the pipeline

- **Important notes:** 
	+ Scripts are intended to be used with Python 3.11.4, other version could case issues with the execution. Example, type hinting with `list[str]` without the `typing` module is only supported in Python 3.9 and later. 
	+ Run complete full pipeline for all the data took hours and used around 32 GB RAM, as it combine all the datasets from 2016 to 2025, please ensure the are enough resources to run the full datasets.
	+ If there are issues downloding the data from GitHub, please review the data sources section and download manually the datasets for Metro and Capital bike-share.

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
# From repository root
python -m venv .venv
# Activate the venv in Bash terminal
source .venv/bin/activate
pip install -r requirements.txt

# Optionally set your Census API key in the current session (or use a secrets manager)
export US_CENSUS_API_KEY="YOUR_KEY"

# Use the Python orchestrator directly
python -m SourceCode.DataCuration.run_pipeline --raw-data ./RawData --temp-data ./TempData --curated-data ./CuratedData
```

Flags of interest
- `--no-unzip` — skip unzipping archives if raw files are already extracted
- `--skip-demographics` — skip Census calls if you don't have `US_CENSUS_API_KEY`
- `--skip-weather` — skip Open-Meteo weather gathering
- `--features-all` — create features for all monthly trip files instead of a subset

---

## Used system characteristics and specifications

System used to create this project for future reference and reproducibility.

| Characteristic    | Information                                                                              |
| :---------------: | :--------------------------------------------------------------------------------------: | 
| Edition           | Windows 10 Enterprise                                                                    | 
| Version           | 22H2                                                                                     | 
| OS Build          | 19045.6456                                                                               | 
| Processor         | 11th Gen Intel(R) Core(TM) i5-11400H @ 2.70GHz   2.70 GHz                                | 
| Installed RAM     | 32.0 GB (31.7 GB usable)                                                                 | 
| Storage	477 GB  | SSD Micron_2210_MTFDHBA512QFD, 1.82 TB SSD Samsung SSD 970 EVO Plus 2TB                  | 
| Graphics Card     | NVIDIA GeForce RTX 3060 Laptop GPU (6 GB), Intel(R) UHD Graphics (128 MB)                | 
| System Type	    | 64-bit operating system, x64-based processor                                             | 

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
-  Prohibited use and conduct:
    + Use the data or analysis in any unlawful manner or for any unlawful purpose
    + Attempt to correlate the data with names, addresses, or other personally identifiable information

---
## Data Sources

- Metro Bike Share. (2024). Metro Bike Share Trip Data [Dataset]. Los Angeles Metropolitan Transportation Authority. [Metro Bike-share data](https://bikeshare.metro.net/about/data/)
- Capital Bikeshare. (2024). Capital Bikeshare Trip History Data [Dataset]. Lyft Bikes and Scooters, LLC. [Capital Bike-share data](https://s3.amazonaws.com/capitalbikeshare-data/index.html)
- Open-Meteo. (n.d.). Open-Meteo Weather API Database [Dataset]. [Open Meteo data](https://open-meteo.com/en/docs/historical-weather-api) (Licensed under CC BY 4.0)
- U.S. Census Bureau. (2023). American Community Survey 5-Year Estimates, Table [S0101] [Dataset]. U.S. Department of Commerce.  [U.S. Census ACS 5 year data](https://www.census.gov/data/developers/data-sets/acs-5year.html)

---

## Contact & License

For questions, please use the repo issue tracker or contact the repository owner.

The data in this repository comes from public sources; check the data providers' license terms. Clarify licensing before reuse.

---

## Note on Data Use & Redistribution

This curated dataset includes raw trip data from Metro Bike Share (Los Angeles) and Capital Bikeshare (Washington, D.C.), combined with weather and U.S. Census demographic data. 

- Metro Bike Share data: redistribution and reuse are subject to Metro’s Terms of Use. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Metro - Terms and conditions](https://bikeshare.metro.net/terms-and-conditions/)

- Capital Bikeshare data: redistribution and reuse are subject to Capital License Agreement. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution. Capital license agreement allows to include the data as source material, in analyses, report, or studies pushblised or distributed for non-comercial purposes. Please review [Capital - Data License Agreement](https://capitalbikeshare.com/data-license-agreement)

- Open-Meteo weather data: redistribution and reuse are under CC BY 4.0; users must provide attribution. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Terms of use](https://open-meteo.com/en/terms)

- U.S. Census ACS data: This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Terms of Service](https://www.census.gov/data/developers/about/terms-of-service.html)

---

## How to add more cities

To expand to additional cities, add: (1) zip/csv acquisition functions in `RawData/` and data acquisition scripts, (2) extend the `data_cleaning_homogenization` function to include vendor-specific logic and (3) update stations mapping and DEM/Census steps to include the new city's lat/lon census linkage.

---

## Changes history

- 2025-12-02: New documentation and metadata added (this file)

