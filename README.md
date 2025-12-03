# Data Curation

This project is to create a curated dataset and a reproducible workflow that integrates data from bike-sharing trips in 
Washington, D.C. (Capital Bikeshare) and Los Angeles (Metro Bikeshare), and to add contextual weather data (Open-Meteo) 
and demographic data (U.S. Census ACS), utilizing the USGS Science Data Life Cycle framework.

For comprehensive documentation, see `DATA_CURATION.md` in the repository root. That file provides an expanded overview of the pipeline, outputs, and reproducibility resources.

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
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m SourceCode.DataCuration.run_pipeline --raw-data ./RawData --temp-data ./TempData --curated-data ./CuratedData
```

Notes & useful flags
- `--no-unzip` — skip unzipping archives if the raw files are already extracted.
- `--skip-demographics` — skip US Census calls if you don't have a `US_CENSUS_API_KEY`.
- `--skip-weather` — skip weather assembly in the cleaning step (useful for quick local runs).
- `--features-all` — run feature engineering for all curated monthly trip files.

Dependencies
- See `requirements.txt` for the Python packages used by the pipeline.
- The Python version used to create the results was 3.11.4

Troubleshooting
- `uszipcode` may download a zipcode DB on first use — ensure your environment can write to disk.
- If your editor shows unresolved imports but the code runs inside the venv, make sure your editor uses the venv Python interpreter.

