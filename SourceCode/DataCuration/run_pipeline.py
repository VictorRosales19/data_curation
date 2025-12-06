"""Orchestrator for the data_curation pipeline.

This script runs the main steps in order:
  1) Data acquisition (unzip, Open-Meteo)
  2) Data cleaning & homogenization
  3) Feature engineering (per-month features)

It calls the module-level mains from the component modules so each step
keeps its own CLI/options while allowing a single entrypoint for the full run.
"""

import argparse
import sys

import SourceCode.DataCuration.data_acquisition as data_acquisition
import SourceCode.DataCuration.data_cleaning_homogenization as clean_homogenize
import SourceCode.DataCuration.feature_engineering as feat_enginering


def main(argv: list[str] | None = None) -> None:
    """Main function to run the full data curation pipeline end-to-end.

    Parameters
    ----------
    argv : list[str] | None, optional
        Command-line arguments to parse, by default None
    """    
    p = argparse.ArgumentParser(description="Run full data curation pipeline end-to-end")
    p.add_argument("--raw-data", default="./RawData")
    p.add_argument("--temp-data", default="./TempData")
    p.add_argument("--curated-data", default="./CuratedData")
    p.add_argument("--start-date", default="2016-01-01")
    p.add_argument("--end-date", default="2025-10-01")
    p.add_argument("--no-unzip", action="store_true", help="Skip unzipping archives in RawData")
    p.add_argument("--skip-demographics", action="store_true", help="Skip demographics step in cleaning")
    p.add_argument("--skip-weather", action="store_true", help="Skip weather assembly in cleaning")
    p.add_argument("--features-all", action="store_true", help="Run feature engineering for all monthly files")
    p.add_argument("--features-year", type=int, help="Run feature engineering for a specific year (requires --features-month)")
    p.add_argument("--features-month", type=int, help="Run feature engineering for a specific month (1-12)")
    p.add_argument("--sleep", type=int, default=70, help="Sleep seconds between Open-Meteo requests")
    args = p.parse_args(argv)

    raw = args.raw_data
    temp = args.temp_data
    curated = args.curated_data

    # 1) Data acquisition
    print("== Step 1: Data acquisition ==")
    daq_args = [
        "--raw-data", raw,
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--sleep", str(args.sleep),
    ]
    if args.no_unzip:
        daq_args.append("--no-unzip")

    try:
        # call the module main to reuse its CLI logic (keeps behavior consistent)
        data_acquisition.main(daq_args)
    except SystemExit as se:
        # argparse in data_acquisition.main may call sys.exit; intercept to continue
        if se.code != 0:
            print(f"Data acquisition failed with exit code: {se.code}")
            raise
    except Exception as e:
        print(f"Data acquisition failed: {e}")
        raise

    # 2) Data cleaning & homogenization
    print("== Step 2: Data cleaning & homogenization ==")
    clean_args = [
        "--raw-data", raw,
        "--temp-data", temp,
        "--curated-data", curated,
    ]
    if args.skip_demographics:
        clean_args.append("--skip-demographics")
    if args.skip_weather:
        clean_args.append("--skip-weather")

    # call module main to reuse its CLI logic
    try:
        clean_homogenize.main(clean_args)
    except SystemExit as se:
        # argparse in clean.main may call sys.exit; intercept to continue
        if se.code != 0:
            print(f"Cleaning failed with exit code: {se.code}")
            raise
    except Exception as e:
        print(f"Cleaning step failed: {e}")
        raise

    # 3) Feature engineering
    print("== Step 3: Feature engineering ==")
    fe_args: list[str] = ["--curated-data", curated]
    if args.features_all:
        fe_args.append("--all")
    elif args.features_year and args.features_month:
        fe_args.extend(["--year", str(args.features_year), "--month", str(args.features_month)])
    else:
        # default: run all
        fe_args.append("--all")

    try:
        feat_enginering.main(fe_args)
    except SystemExit as se:
        if se.code != 0:
            print(f"Feature engineering failed with exit code: {se.code}")
            raise
    except Exception as e:
        print(f"Feature engineering failed: {e}")
        raise

    print("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
