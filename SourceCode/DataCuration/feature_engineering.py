"""Data acquisition utilities for the data_curation pipeline.
This module includes functions to perform feature engineering on trip data.
It is designed to be used as part of an end-to-end data curation process.
It can be executed as a standalone script or imported as a module.
"""

import pandas as pd
import numpy as np
import argparse
import glob
import os

from SourceCode.utils import ensure_dir


def generate_trips_feature_engineering(df_trip_month:pd.DataFrame, df_stations:pd.DataFrame) -> pd.DataFrame:
    """Generate feature engineering for trip data of a specific month. 
    This is done by adding time-based and distance-based features to the trip data.
    Another consideration is monthly data to avoid running into memory issues.

    Parameters
    ----------
    df_trip_month : pd.DataFrame
        Homoginized trip data for a specific month
    df_stations : pd.DataFrame
        Station data including coordinates

    Returns
    -------
    pd.DataFrame
        DataFrame with engineered features for the trip data
    """    
    trip_used_columns = [
        'trip_uuid', 
        'start_time', 
        'end_time', 
        'member_type', 
        'duration', 
        'start_station_uuid', 
        'end_station_uuid', 
        'bike_uuid'
    ]

    df_trip_month = df_trip_month[trip_used_columns]

    # add coordinate information
    df_trip_month_feat_eng = pd.merge(df_trip_month, df_stations, left_on="start_station_uuid", right_on="station_uuid")
    df_trip_month_feat_eng = pd.merge(df_trip_month_feat_eng, df_stations, left_on="end_station_uuid", right_on="station_uuid", suffixes=("_start", "_end"))
    
    # add time-based features
    df_trip_month_feat_eng[ "start_time"] = pd.to_datetime(df_trip_month_feat_eng["start_time"], format="mixed")
    df_trip_month_feat_eng[ "end_time"] = pd.to_datetime(df_trip_month_feat_eng["end_time"], format="mixed")
    
    duration = (df_trip_month_feat_eng[ "end_time"] - df_trip_month_feat_eng[ "start_time"]).dt.total_seconds()
    df_trip_month_feat_eng["duration"] = duration
    
    df_trip_month_feat_eng["trip_duration_category"] = 0                                             # short 
    df_trip_month_feat_eng.loc[(duration >= 300) & (duration < 600), "trip_duration_category"] = 1   # medium
    df_trip_month_feat_eng.loc[duration >= 600, "trip_duration_category"] = 2                        # long
    
    df_trip_month_feat_eng["hour_of_day"] = df_trip_month_feat_eng["start_time"].dt.hour
    df_trip_month_feat_eng["trip_hour_category"] = df_trip_month_feat_eng["hour_of_day"]//4
    df_trip_month_feat_eng["day_of_week"] = df_trip_month_feat_eng["start_time"].dt.dayofweek
    df_trip_month_feat_eng["is_weekend"] = df_trip_month_feat_eng["day_of_week"] >= 5
    df_trip_month_feat_eng["month"] = df_trip_month_feat_eng["start_time"].dt.month
    
    # Create a dictionary to map month numbers to seasons
    month_to_season = {
        1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
        5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Autumn', 10: 'Autumn', 11: 'Autumn', 12: 'Winter'
    }
    # Map the month from the 'Date' column to a new 'Season' column
    df_trip_month_feat_eng["season"] = df_trip_month_feat_eng["month"].map(month_to_season)
    
    # add distance-based features
    distance = haversine_vectorized(df_trip_month_feat_eng)
    df_trip_month_feat_eng["distance_km"] = distance
    df_trip_month_feat_eng["average_speed_kmh"] = distance/duration * 3600
    speed_limit_condition = df_trip_month_feat_eng["average_speed_kmh"] <= 50 # bikes cannot go more than 50 km/h 
    df_trip_month_feat_eng = df_trip_month_feat_eng.loc[speed_limit_condition] 
    
    df_trip_month_feat_eng["trip_distance_category"] = 0                                         # short 
    df_trip_month_feat_eng.loc[(distance >= 2) & (distance < 4), "trip_distance_category"] = 1   # medium
    df_trip_month_feat_eng.loc[distance >= 4, "trip_distance_category"] = 2                      # long

    return df_trip_month_feat_eng


def haversine_vectorized(df_trips:pd.DataFrame) -> pd.Series:
    """Calculate Haversine distances between start and end coordinates for trips.
    The Haversine formula calculates the great-circle distance between two points on a sphere given their longitudes and latitudes.

    Parameters
    ----------
    df_trips : pd.DataFrame
        Homoginized trip data with start and end coordinates

    Returns
    -------
    pd.Series
        Series containing the Haversine distances in kilometers between start and end points
    """    
    # Approximate Earth radius in kilometers
    R = 6371

    # Convert latitude and longitude from degrees to radians
    lat_start_rad = np.radians(df_trips["latitude_start"])
    lon_start_rad = np.radians(df_trips["longitude_start"])
    lat_end_rad = np.radians(df_trips["latitude_end"])
    lon_end_rad = np.radians(df_trips["longitude_end"])

    # Calculate differences
    dlat = lat_end_rad - lat_start_rad
    dlon = lon_end_rad - lon_start_rad

    # Haversine formula
    a = np.sin(dlat / 2.0)**2 + np.cos(lat_start_rad) * np.cos(lat_end_rad) * np.sin(dlon / 2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    distance_km = R * c
    
    return distance_km


##########################################################################################
####                                        Main                                       ###
##########################################################################################

def main(argv: list[str] | None = None) -> None:
    """Main function to execute feature engineering on monthly trip files.

    Usage examples:
      - Process a single month:
          python -m SourceCode.DataCuration.feature_engineering --year 2020 --month 1
      - Process all curated monthly trips in CuratedData:
          python -m SourceCode.DataCuration.feature_engineering --all

    Parameters
    ----------
    argv : list[str] | None, optional
        Command-line arguments to parse, by default None

    Raises
    ------
    FileNotFoundError
        Raised if the stations file or trips file is not found.
    ValueError
        Raised if neither --all nor both --year and --month are provided.
    FileNotFoundError
        Raised if the specified trips file does not exist.
    """    
    p = argparse.ArgumentParser(description="Feature engineering for monthly trips")
    p.add_argument("--curated-data", default="./CuratedData", help="Curated data folder (default ./CuratedData)")
    p.add_argument("--year", type=int, help="Year to process (e.g. 2020)")
    p.add_argument("--month", type=int, help="Month to process (1-12)")
    p.add_argument("--all", action="store_true", help="Process all trips_YYYY_MM.csv files in the curated folder")
    args = p.parse_args(argv)

    curated = args.curated_data
    ensure_dir(curated)

    stations_path = os.path.join(curated, "stations.csv")
    if not os.path.exists(stations_path):
        raise FileNotFoundError(f"Stations file not found: {stations_path}. Run the homogenization step first.")

    df_stations = pd.read_csv(stations_path)

    files_to_process = []
    if args.all:
        pattern = os.path.join(curated, "trips_*.csv")
        files_to_process = sorted(glob.glob(pattern))
    else:
        if args.year is None or args.month is None:
            raise ValueError("Either --all or both --year and --month must be provided")
        filename = os.path.join(curated, f"trips_{args.year}_{args.month:02d}.csv")
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Trips file not found: {filename}")
        files_to_process = [filename]

    if not files_to_process:
        print("No files to process")
        return

    for file in files_to_process:
        print(f"Processing {file}")
        df_trip_month = pd.read_csv(file)

        df_features = generate_trips_feature_engineering(df_trip_month, df_stations)

        # Derive year/month from filename to keep consistent naming
        base = os.path.basename(file)

        try:
            parts = base.replace("trips_", "").replace(".csv", "").split("_")
            y = int(parts[0])
            m = int(parts[1])
        except Exception:
            # fallback to month from data
            df_features["start_time"] = pd.to_datetime(df_features["start_time"], errors="coerce")
            df_features = df_features.dropna(subset=["start_time"]).copy()
            y = int(df_features["start_time"].dt.year.mode().iat[0])
            m = int(df_features["start_time"].dt.month.mode().iat[0])

        out_name = os.path.join(curated, f"features_{y}_{m:02d}.csv")
        df_features.to_csv(out_name, index=False)
        print(f"Saved features to {out_name}")


if __name__ == "__main__":
    main()