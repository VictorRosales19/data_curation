# Data Curation

## Preject Overview
This project is to create a curated dataset and a reproducible workflow that integrates data from bike-sharing trips in 
Washington, D.C. (Capital Bikeshare) and Los Angeles (Metro Bikeshare), and to add contextual weather data (Open-Meteo) 
and demographic data (U.S. Census ACS), utilizing the USGS Science Data Life Cycle framework.

For comprehensive documentation, see `DATA_CURATION.md` in the repository root. That file provides an expanded overview of the pipeline, outputs, and reproducibility resources.

---

## Content Overview

Quick contents
- `RawData/` - place raw zip/csvs here. Subfolders used by the scripts:
	- `RawData/MetroBike`
	- `RawData/CapitalBike`
	- `RawData/OpenMeteo` (weather outputs)
- `TempData/` - intermediate files
- `CuratedData/` - final outputs: `stations.csv`, `bikes.csv`, `trips_{YYYY}_{MM}.csv`, `features_{YYYY}_{MM}.csv`

Key scripts
- `SourceCode/DataCuration/data_acquisition.py` — unzip helpers and Open-Meteo / Census request helpers.
- `SourceCode/DataCuration/data_cleaning_homogenization.py` — homogenizes vendor formats and writes curated outputs.
- `SourceCode/DataCuration/feature_engineering.py` — per-month feature generation.
- `SourceCode/DataCuration/run_pipeline.py` — orchestrator that runs acquisition → cleaning → features.
- `SourceCode/DataCuration/automated_workflow.ps1` — optional Windows PowerShell launcher that calls the Python orchestrator.

---

## How to run data curation workflow (Sample datasets - Enable quicker runs)

- **Important notes:** 
	+ Scripts are intended to be used with Python 3.11.4, other version could case issues with the execution. Example, type hinting with `list[str]` without the `typing` module is only supported in Python 3.9 and later. 
	+ Running the sample datasets is faster and less computational expensive than the full datasets; however, it takes around **20 minutes** using the *system characteristics and specifications* mentioned in `DATA_CURATION.md`.
	+ The quick run uses the folders `RawDataExample`, `TempDataExample`, and `CuratedDataExample` to reproduce the data curation workflow using a significantly smaller sample of all available data.
	+ If there are issues downloding the data from GitHub, please review the data sources section and download manually the datasets for Metro and Capital bike-share.

Getting started (Windows PowerShell)
```powershell
# From repository root
python -m venv .venv
# Activate the venv in PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Optionally set your Census API key in the current session (or use a secrets manager)
$env:US_CENSUS_API_KEY = "YOUR_KEY"

# Run the Python orchestrator directly
python -m SourceCode.DataCuration.run_pipeline --raw-data .\RawDataExample --temp-data .\TempDataExample --curated-data .\CuratedDataExample --start-date "2023-01-01" --end-date "2023-04-01"
```

Getting started (macOS / Linux)
```bash
# From repository root
python -m venv .venv
# Activate the venv in Bash terminal
source .venv/bin/activate
pip install -r requirements.txt

# Optionally set your Census API key in the current session (or use a secrets manager)
export US_CENSUS_API_KEY="YOUR_KEY"

# Use the Python orchestrator directly
python -m SourceCode.DataCuration.run_pipeline --raw-data ./RawDataExample --temp-data ./TempDataExample --curated-data ./CuratedDataExample --start-date "2023-01-01" --end-date "2023-04-01"
```

---

## How to run data curation workflow (Full datasets - Time-consuming  and computationally expensive)

- **Important notes:** 
    + Scripts are intended to be used with Python 3.11.4; other versions could cause issues with the execution. Example, type hinting with `list[str]` without the `typing` module is only supported in Python 3.9 and later. 
    + Run the complete pipeline for all the data took **hours** and used around **32 GB RAM**, as it combines all the datasets from 2016 to 2025. Please **ensure** there are **enough resources** to run the complete datasets.
    + If there are issues downloading the data from GitHub, please review the data sources section and download the datasets for Metro and Capital bike-share manually.

Getting started (Windows PowerShell)
```powershell
# From repository root
python -m venv .venv
# Activate the venv in PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Optionally set your Census API key in the current session (or use a secrets manager)
$env:US_CENSUS_API_KEY = "YOUR_KEY"

# Run the full pipeline using the PS1 launcher (recommended on Windows)
.\SourceCode\DataCuration\automated_workflow.ps1

# Or run the Python orchestrator directly
python -m SourceCode.DataCuration.run_pipeline --raw-data .\RawData --temp-data .\TempData --curated-data .\CuratedData
```

Getting started (macOS / Linux)
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

Notes & useful flags
- `--no-unzip` — skip unzipping archives if the raw files are already extracted.
- `--skip-demographics` — skip US Census calls if you don't have a `US_CENSUS_API_KEY`; however, all derivate data and further analysis will not work as intended.
- `--skip-weather` — skip weather assembly in the cleaning step.
- `--features-all` — run feature engineering for all curated monthly trip files.

Dependencies
- See `requirements.txt` for the Python packages used by the pipeline.
- The Python version used to create the results was 3.11.4 in a Windows Operating System.

Troubleshooting
- `uszipcode` may download a zipcode DB on first use — ensure your environment can write to disk.
- If your editor shows unresolved imports but the code runs inside the venv, make sure your editor uses the venv Python interpreter.

---

## Limitations & Notes

- Data quality: The pipeline attempts to discard anomalous trips (unrealistic speed) — check `feature_engineering.py` for thresholds.
- Scope: Only Washington D.C. (Capital Bikeshare) and Los Angeles (Metro) are currently implemented.
- Census calls: `uszipcode` or US Census requests are used to fetch demographics; running without an API key will disable ACS calls.
-  Prohibited use and conduct:
    + Use the data or analysus in any unlawful manner or for any unlawful purpose
    + Attempt to correlate the data with names, addresses, or other personally identifiable information

---

## Data Sources

- Metro Bike Share. (2024). Metro Bike Share Trip Data [Dataset]. Los Angeles Metropolitan Transportation Authority. [Metro Bike-share data](https://bikeshare.metro.net/about/data/)
- Capital Bikeshare. (2024). Capital Bikeshare Trip History Data [Dataset]. Lyft Bikes and Scooters, LLC. [Capital Bike-share data](https://s3.amazonaws.com/capitalbikeshare-data/index.html)
- Open-Meteo. (n.d.). Open-Meteo Weather API Database [Dataset]. [Open Meteo data](https://open-meteo.com/en/docs/historical-weather-api) (Licensed under CC BY 4.0)
- U.S. Census Bureau. (2023). American Community Survey 5-Year Estimates, Table [S0101] [Dataset]. U.S. Department of Commerce.  [U.S. Census ACS 5 year data](https://www.census.gov/data/developers/data-sets/acs-5year.html)

---

## Note on Data Use & Redistribution

This curated dataset includes raw trip data from Metro Bike Share (Los Angeles) and Capital Bikeshare (Washington, D.C.), combined with weather and U.S. Census demographic data. 

- Metro Bike Share data: redistribution and reuse are subject to Metro’s Terms of Use. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Metro - Terms and conditions](https://bikeshare.metro.net/terms-and-conditions/)

- Capital Bikeshare data: redistribution and reuse are subject to Capital License Agreement. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution. Capital license agreement allows to include the data as source material, in analyses, report, or studies pushblised or distributed for non-comercial purposes. Please review [Capital - Data License Agreement](https://capitalbikeshare.com/data-license-agreement)

- Open-Meteo weather data: redistribution and reuse are under CC BY 4.0; users must provide attribution. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Terms of use](https://open-meteo.com/en/terms)

- U.S. Census ACS data: This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau. Users of this dataset are responsible for verifying and complying with those terms before any public redistribution or commercial use. Please review [Terms of Service](https://www.census.gov/data/developers/about/terms-of-service.html)
