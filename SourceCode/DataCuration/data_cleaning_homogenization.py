"""Data acquisition utilities for the data_curation pipeline.
This module includes functions to fetch bike-share trip data from different file sources,
clean and homogenize the data, and prepare it for further analysis.
It is designed to be used as part of an end-to-end data curation process.
It can be executed as a standalone script or imported as a module.
"""

from narwhals import List
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge

import pandas as pd
import numpy as np
import argparse
import random
import time
import uuid
import os

from SourceCode.DataCuration.data_acquisition import request_census_data, get_zip_code_by_coordenates
from SourceCode.utils import ensure_dir

##########################################################################################
####                                   Homogenization                                  ###
##########################################################################################

def homogenize_metro_dataset(raw_data_folder:str) -> pd.DataFrame:
    """Homogize MetroBike dataset to common schema.

    Parameters
    ----------
    raw_data_folder : str
        Folder where the raw data is stored

    Returns
    -------
    pd.DataFrame
        Dataset homogenized from MetroBike dataset
    """    
    metro_name_map = {
        "trip_id": "trip_id",
        "duration": "duration",
        "start_time": "start_time",
        "end_time": "end_time",
        "start_station_id": "start_station_id",
        "start_lat": "start_lat",
        "start_lon": "start_lon",
        "end_station_id": "end_station_id",
        "end_lat": "end_lat",
        "end_lon": "end_lon",
        "bike_id": "bike_id",
        "plan_duration": "plan_duration",
        "trip_route_category": "trip_route_category",
        "passholder_type": "member_type",
        "bike_type": "bike_type",
        "start station name": "start_station_name",
        "end station name": "end_station_name",
    }
    
    metro_folder = f"{raw_data_folder}/MetroBike"
    csv_file_list = [file for file in os.listdir(metro_folder) if file.endswith(".csv") and "trips" in file]

    df_metro = pd.DataFrame()

    for csv_file in csv_file_list:
        print(csv_file)
        csv_file_path = f"{metro_folder}/{csv_file}"
        df_metro_segment = pd.read_csv(csv_file_path)

        if "start_station" in df_metro_segment.columns:
            df_metro_segment = df_metro_segment.rename(columns={"start_station":"start_station_id", "end_station":"end_station_id"})
            
        df_metro = pd.concat([df_metro, df_metro_segment])

    df_metro = df_metro.rename(columns=metro_name_map)
    df_metro = df_metro.sort_values(by=["start_time"])
    df_metro = df_metro.reset_index(drop=True)

    return df_metro


def homogenize_capital_dataset(raw_data_folder:str) -> pd.DataFrame:
    """Homogize CapitalBike dataset to common schema.

    Parameters
    ----------
    raw_data_folder : str
        Folder where the raw data is stored

    Returns
    -------
    pd.DataFrame
        Dataset homogenized from CapitalBike dataset
    """    
    capital_name_map = {
        "Duration": "duration",
        "started_at": "start_time",
        "ended_at": "end_time",
        "start_station_id": "start_station_id",
        "start_station_name": "start_station_name",
        "end_station_id": "end_station_id",
        "end_station_name": "end_station_name",
        "Bike number": "bike_id",
        "member_casual": "member_type",
        "ride_id": "trip_id",
        "rideable_type": "bike_type",
        "start_lat": "start_lat",
        "start_lng": "start_lon",
        "end_lat": "end_lat",
        "end_lng": "end_lon",
    }

    capital_folder = f"{raw_data_folder}/CapitalBike"
    csv_file_list = [file for file in os.listdir(capital_folder) if file.endswith(".csv") and "trip" in file]

    df_capital = pd.DataFrame()

    for csv_file in csv_file_list:
        print(csv_file)
        csv_file_path = f"{capital_folder}/{csv_file}"
        df_capital_segment = pd.read_csv(csv_file_path)

        if "Start station number" in df_capital_segment.columns:
            df_capital_segment = df_capital_segment.rename(
                columns={
                    "Start station number":"start_station_id", 
                    "End station number":"end_station_id"
                }
            )

        if "Start station" in df_capital_segment.columns:
            df_capital_segment = df_capital_segment.rename(
                columns={
                    "Start station":"start_station_name", 
                    "End station":"end_station_name"
                }
            )

        if "Start date" in df_capital_segment.columns:
            df_capital_segment = df_capital_segment.rename(
                columns={
                    "Start date":"started_at", 
                    "End date":"ended_at"
                }
            )

        if "Member type" in df_capital_segment.columns:
            df_capital_segment = df_capital_segment.rename(
                columns={"Member type":"member_casual"}
            )
            
        df_capital = pd.concat([df_capital, df_capital_segment])

    df_capital = df_capital.rename(columns=capital_name_map)
    df_capital = df_capital.sort_values(by=["start_time"])
    df_capital = df_capital.reset_index(drop=True)

    # add bike IDs for missing bike IDs
    df_capital.loc[(df_capital["bike_id"].isna()) & (df_capital["bike_type"]=="classic_bike"), "bike_id"] = "0000_classic"   # unknown
    df_capital.loc[(df_capital["bike_id"].isna()) & (df_capital["bike_type"]=="electric_bike"), "bike_id"] = "0000_electric" # unknown
    df_capital.loc[(df_capital["bike_id"].isna()) & (df_capital["bike_type"]=="docked_bike"), "bike_id"] = "0000_docked"     # unknown

    return df_capital


##########################################################################################
####                                      Stations                                     ###
##########################################################################################

def generate_stations_dataframe(df_metro: pd.DataFrame, df_capital: pd.DataFrame) -> pd.DataFrame:
    """Generate a combined stations DataFrame from both MetroBike and CapitalBike datasets.

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike DataFrame
    df_capital : pd.DataFrame
        Homogenized CapitalBike DataFrame

    Returns
    -------
    pd.DataFrame
        Combined stations DataFrame from both MetroBike and CapitalBike datasets
    """    
    df_stations_metro = generate_metro_stations_dataframe(df_metro)
    df_stations_capital = generate_capital_stations_dataframe(df_capital)

    df_stations = pd.concat([df_stations_metro, df_stations_capital])
    df_stations = df_stations.reset_index(drop=True)

    return df_stations


def generate_metro_stations_dataframe(df_metro:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all stations in MetroBike dataset

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike DataFrame

    Returns
    -------
    pd.DataFrame
        Dataset with all stations in MetroBike dataset
    """    
    start_station_columns = [
        'start_station_id',
        'start_lat', 
        'start_lon', 
        'start_station_name'
    ]
    end_station_columns = [
        'end_station_id', 
        'end_lat', 
        'end_lon',
        'end_station_name'
    ]

    station_columns = start_station_columns + end_station_columns
    df_stations = df_metro[station_columns].drop_duplicates()

    # get stations from start and end
    df_stations_start = df_stations[start_station_columns]
    df_stations_start.columns = df_stations_start.columns.str.replace("start_", "")
    df_stations_start = get_metro_unique_stations(df_stations_start)

    df_stations_end = df_stations[end_station_columns]
    df_stations_end.columns = df_stations_end.columns.str.replace("end_", "")
    df_stations_end = get_metro_unique_stations(df_stations_end)

    # combine both stations (start, end)
    df_stations = pd.concat([df_stations_start, df_stations_end])
    df_stations = df_stations.drop_duplicates()
    df_stations = df_stations.dropna(subset=["lat", "lon"])
    df_stations = df_stations.groupby(by=["station_id"]).agg(
        lat=('lat', safe_mode),
        lon=('lon', safe_mode),
        station_name=('station_name', 'first')
    )
    df_stations = df_stations.reset_index()

    # add zip codes
    df_stations = rectify_coordinates(df_stations, city_latitude=34.059753, city_longitude=-118.2375)
    zip_dict = get_zip_code_by_coordenates(df_stations)
    df_stations["zip_code"] = zip_dict

    # add city
    df_stations["city"] = "Los Angeles"

    # add unique identifier
    seed_value = 0
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_stations))]
    df_stations.insert(0, 'station_uuid', uuids)

    # rename columns for homogeneity
    df_stations_metro = df_stations.rename(
        columns={
            "station_id": "original_station_id",
            "lat": "latitude",
            "lon": "longitude"
        }
    )

    return df_stations_metro


def get_metro_unique_stations(df_stations_terminal: pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with Metro unique stations for start or end of the trip

    Parameters
    ----------
    df_stations_terminal : pd.DataFrame
        DataFrame with stations at the start or end of the trip

    Returns
    -------
    pd.DataFrame
        DataFrame with unique Metro stations at the start or end of the trip
    """    
    df_stations_terminal = df_stations_terminal.drop_duplicates()
    df_stations_terminal = df_stations_terminal.sort_values(by=["station_id"])
    df_stations_terminal = df_stations_terminal.groupby(by=["station_id"]).agg(
        lat=('lat', safe_mode),
        lon=('lon', safe_mode),
        station_name=('station_name', 'first')
    )
    df_stations_terminal = df_stations_terminal.reset_index()

    return df_stations_terminal


def generate_capital_stations_dataframe(df_capital:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all stations in CapitalBike dataset

    Parameters
    ----------
    df_capital : pd.DataFrame
        Homogenized CapitalBike DataFrame

    Returns
    -------
    pd.DataFrame
        Dataset with all stations in CapitalBike dataset
    """      
    start_station_columns = [
        'start_station_id',
        'start_lat', 
        'start_lon', 
        'start_station_name'
    ]
    end_station_columns = [
        'end_station_id', 
        'end_lat', 
        'end_lon',
        'end_station_name'
    ]
    
    station_columns = start_station_columns + end_station_columns
    df_stations = df_capital[station_columns].drop_duplicates()
    
    # get stations from start and end
    df_stations_start = df_stations[start_station_columns].copy()
    df_stations_start.columns = df_stations_start.columns.str.replace("start_", "")
    df_stations_start = get_capital_unique_stations(df_stations_start)
    
    df_stations_end = df_stations[end_station_columns].copy()
    df_stations_end.columns = df_stations_end.columns.str.replace("end_", "")
    df_stations_end = get_capital_unique_stations(df_stations_end)
    
    # combine both stations (start, end)
    df_stations = pd.concat([df_stations_start, df_stations_end])
    df_stations = df_stations.drop_duplicates()
    df_stations = df_stations.dropna(subset=["lat", "lon"])
    df_stations = df_stations.groupby(by=["station_id"]).agg(
        lat=('lat', safe_mode),
        lon=('lon', safe_mode),
        station_name=('station_name', 'first')
    )
    df_stations = df_stations.reset_index()
    
    # add zip codes
    df_stations = rectify_coordinates(df_stations, city_latitude=38.910366, city_longitude=-77.07251)
    zip_dict = get_zip_code_by_coordenates(df_stations)
    df_stations["zip_code"] = zip_dict
    
    # add city
    df_stations["city"] = "Washington D.C."

    # add unique identifier
    seed_value = 1
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_stations))]
    df_stations.insert(0, 'station_uuid', uuids)

    # rename columns for homogeneity
    df_stations_capital = df_stations.rename(
        columns={
            "station_id": "original_station_id",
            "lat": "latitude",
            "lon": "longitude"
        }
    )

    return df_stations_capital


def get_capital_unique_stations(df_stations_terminal:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with Capital unique stations for start or end of the trip

    Parameters
    ----------
    df_stations_terminal : pd.DataFrame
        DataFrame with stations at the start or end of the trip

    Returns
    -------
    pd.DataFrame
        DataFrame with unique Capital stations at the start or end of the trip
    """    
    df_stations_terminal["lat"] = df_stations_terminal["lat"].astype(str)
    df_stations_terminal["lon"] = df_stations_terminal["lon"].astype(str)
    
    valid_lat_mask = df_stations_terminal['lat'].str.count(r'\d') > 6 # valid latitude
    valid_lon_mask = df_stations_terminal['lon'].str.count(r'\d') > 6 # valid longitude
    df_stations_terminal = df_stations_terminal.loc[valid_lat_mask & valid_lon_mask]
    
    df_stations_terminal.loc[:, "lat"] = df_stations_terminal.loc[:, "lat"].astype(float)
    df_stations_terminal.loc[:, "lon"] = df_stations_terminal.loc[:, "lon"].astype(float)
    
    df_stations_terminal = df_stations_terminal.groupby(by=["lat", "lon"]).first()
    df_stations_terminal = df_stations_terminal.reset_index()
    
    df_stations_terminal.loc[:, "station_id"] = df_stations_terminal["station_id"].astype(int)
    
    df_stations_terminal = df_stations_terminal.groupby(by=["station_id"]).agg(
        lat=('lat', safe_mode),
        lon=('lon', safe_mode),
        station_name=('station_name', 'first')
    )
    df_stations_terminal = df_stations_terminal.reset_index()

    return df_stations_terminal


def map_stations_foreign_keys_to_uuids(df_stations:pd.DataFrame, df_demographics:pd.DataFrame) -> pd.DataFrame:
    """Map original foreign keys (zip codes) to generated uuids

    Parameters
    ----------
    df_stations : pd.DataFrame
        Dataset with all stations from both datasets
    df_demographics : pd.DataFrame
        Dataset with demographics information including zip code UUIDs

    Returns
    -------
    pd.DataFrame
        Dataset with stations including mapped zip code UUIDs
    """    
    # ensure zip codes are strings with leading zeros if needed
    df_demographics["zip_code"] = df_demographics["zip_code"].astype(str).str.zfill(5)
    df_stations["zip_code"] = df_stations["zip_code"].astype(str).str.zfill(5)
    
    # Map values for Zip Codes
    value_map = df_demographics.set_index("zip_code")["zip_code_uuid"].to_dict()
    df_stations["zip_code_uuid"] = df_stations["zip_code"].map(value_map)
    df_stations = df_stations.drop(columns=["zip_code"])

    return df_stations

##########################################################################################
####                                       Bikes                                       ###
##########################################################################################

def generate_bikes_dataframe(df_metro:pd.DataFrame, df_capital:pd.DataFrame) -> pd.DataFrame: 
    """Generate a combined bikes DataFrame from both MetroBike and CapitalBike datasets.

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike dataset
    df_capital : pd.DataFrame
        Homogenized CapitalBike dataset

    Returns
    -------
    pd.DataFrame
        Combined dataset with bikes from both Metro and Capital datasets
    """    
    df_bikes_metro = generate_metro_bikes_dataframe(df_metro)
    df_bikes_capital = generate_capital_bikes_dataframe(df_capital)

    df_bikes = pd.concat([df_bikes_metro, df_bikes_capital])
    df_bikes = df_bikes.reset_index(drop=True)

    return df_bikes


def generate_metro_bikes_dataframe(df_metro:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all bikes from the MetroBike dataset.

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike dataset

    Returns
    -------
    pd.DataFrame
        Dataset with all bikes from the MetroBike dataset
    """    
    df_bikes = df_metro[["bike_id", "bike_type"]].drop_duplicates()
    df_bikes["bike_id"] = df_bikes["bike_id"].astype(str)
    
    df_bikes = df_bikes.groupby(by=["bike_id"]).agg(safe_mode)
    df_bikes = df_bikes.reset_index()
    
    # remove the IDs that looks as testing IDs
    valid_ids_mask = df_bikes['bike_id'].str.count(r'\d') >= 4
    df_bikes = df_bikes.loc[valid_ids_mask]
    
    # add unique identifier
    seed_value = 2
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_bikes))]
    df_bikes.insert(0, 'bike_uuid', uuids)
    
    # rename columns for homogeneity
    df_bikes_metro = df_bikes.rename(
        columns={
            "bike_id": "original_bike_id"
        }
    )
    
    return df_bikes_metro


def generate_capital_bikes_dataframe(df_capital:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all bikes from the CapitalBike dataset.

    Parameters
    ----------
    df_capital : pd.DataFrame
        Homogenized CapitalBike dataset

    Returns
    -------
    pd.DataFrame
        Dataset with all bikes from the CapitalBike dataset
    """    
    df_bikes = df_capital[["bike_id", "bike_type"]].drop_duplicates()
    df_bikes["bike_id"] = df_bikes["bike_id"].astype(str)
    
    df_bikes = df_bikes.groupby(by=["bike_id"]).agg(safe_mode)
    df_bikes = df_bikes.reset_index()
    
    # remove the IDs that looks as testing IDs
    valid_ids_mask = df_bikes['bike_id'].str.count(r'\d') >= 4
    df_bikes = df_bikes[valid_ids_mask]
    
    # base on before 2020 there were not ebikes and there was bike IDs in the datasets
    df_bikes.loc[df_bikes["bike_type"].isna(), "bike_type"] = "classic_bike" 

    # homogenize bike types with Metro dataset
    df_bikes.loc[df_bikes["bike_type"] == "classic_bike", "bike_type"] = "standard"
    df_bikes.loc[df_bikes["bike_type"] == "docked_bike", "bike_type"] = "standard"
    df_bikes.loc[df_bikes["bike_type"] == "electric_bike", "bike_type"] = "electric"
    
    # add unique identifier
    seed_value = 3
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_bikes))]
    df_bikes.insert(0, 'bike_uuid', uuids)
    
    # rename columns for homogeneity
    df_bikes_capital = df_bikes.rename(
        columns={
            "bike_id": "original_bike_id"
        }
    )

    return df_bikes_capital


##########################################################################################
####                                       Trips                                       ###
##########################################################################################

def generate_trips_dataframe(
        df_metro:pd.DataFrame, 
        df_capital:pd.DataFrame, 
        df_stations_metro:pd.DataFrame = None, 
        df_bikes_metro:pd.DataFrame = None, 
        df_stations_capital:pd.DataFrame = None, 
        df_bikes_capital:pd.DataFrame = None
    ) -> pd.DataFrame:
    """Generate dataset with all homogenized trips from MetroBike and CapitalBike datasets.

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike dataset
    df_capital : pd.DataFrame
        Homogenized CapitalBike dataset
    df_stations_metro : pd.DataFrame, optional
        Dataset with all homogenized MetroBike stations, by default None
    df_bikes_metro : pd.DataFrame, optional
        Dataset with all homogenized MetroBike bikes, by default None
    df_stations_capital : pd.DataFrame, optional
        Dataset with all homogenized CapitalBike stations, by default None
    df_bikes_capital : pd.DataFrame, optional
        Dataset with all homogenized CapitalBike bikes, by default None

    Returns
    -------
    pd.DataFrame
        Dataset with all homogenized trips
    """    
    # allow independence if only trips are the interest
    if df_stations_metro is None:
        df_stations_metro = generate_metro_stations_dataframe(df_metro)
    if df_bikes_metro is None:
        df_bikes_metro = generate_metro_bikes_dataframe(df_metro)
    if df_stations_capital is None:
        df_stations_capital = generate_capital_stations_dataframe(df_capital)
    if df_bikes_capital is None:
        df_bikes_capital = generate_capital_bikes_dataframe(df_capital)

    # generate trip dataframe for each dataset
    df_trips_metro = generate_metro_trips_dataframe(df_metro, df_stations_metro, df_bikes_metro)
    df_trips_capital = generate_capital_trips_dataframe(df_capital, df_stations_capital, df_bikes_capital)

    df_trips = pd.concat([df_trips_metro, df_trips_capital])

    # change to only member and casual
    df_trips.loc[df_trips["member_type"]=="Member", "member_type"] = "member"
    df_trips.loc[df_trips["member_type"]=="Flex Pass", "member_type"] = "member"
    df_trips.loc[df_trips["member_type"]=="Annual Pass", "member_type"] = "member"
    df_trips.loc[df_trips["member_type"]=="Monthly Pass", "member_type"] = "member"
    df_trips.loc[df_trips["member_type"]=="One Day Pass", "member_type"] = "member"
    df_trips.loc[df_trips["member_type"]=="Casual", "member_type"] = "casual"
    df_trips.loc[df_trips["member_type"]=="Walk-up", "member_type"] = "casual"
    df_trips.loc[df_trips["member_type"]=="Testing", "member_type"] = "casual"

    # rename columns for homogeneity
    df_trips = df_trips.rename(
        columns={
            "trip_id": "original_trip_id"
        }
    )

    # add unique identifiers
    seed_value = 4
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_trips))]
    df_trips.insert(0, 'trip_uuid', uuids)

    df_trips = df_trips.sort_values(by=["start_time", "end_time"])
    df_trips = df_trips.reset_index(drop=True)

    return df_trips


def generate_metro_trips_dataframe(df_metro:pd.DataFrame, df_stations_metro:pd.DataFrame, df_bikes_metro:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all valid trips in MetroBike dataset.

    Parameters
    ----------
    df_metro : pd.DataFrame
        Homogenized MetroBike DataFrame
    df_stations_metro : pd.DataFrame
        Dataset with all homogenized MetroBike stations
    df_bikes_metro : pd.DataFrame
        Dataset with all homogenized MetroBike bikes
    Returns
    -------
    pd.DataFrame
        Dataset with all valid MetroBike trips
    """    
    trip_columns = [
        'trip_id', 
        'start_time', 
        'end_time',
        'start_station_id',
        'end_station_id',
        'bike_id',
        'member_type',
        'duration', 
        'plan_duration', 
        'trip_route_category'
    ]

    # only trackable trips
    valid_start_stations_mask = df_metro["start_station_id"].isin(df_stations_metro["original_station_id"])
    valid_end_stations_mask = df_metro["end_station_id"].isin(df_stations_metro["original_station_id"])

    df_metro["bike_id"] = df_metro["bike_id"].astype(str)
    valid_bikes_mask = df_metro["bike_id"].isin(df_bikes_metro["original_bike_id"]) 
    condition = valid_start_stations_mask & valid_end_stations_mask & valid_bikes_mask

    df_trips_metro = df_metro.loc[condition, trip_columns].copy()
    df_trips_metro = df_trips_metro.drop_duplicates()

    # change duration to seconds
    df_trips_metro["duration"] = df_trips_metro["duration"] * 60.0
    df_trips_metro["plan_duration"] = df_trips_metro["plan_duration"] * 60.0

    # change to date time
    df_trips_metro["start_time"] = pd.to_datetime(df_trips_metro["start_time"], format='mixed')
    df_trips_metro["end_time"] = pd.to_datetime(df_trips_metro["end_time"], format='mixed')

    # ensure start time is before end time
    start_quality_condition = df_trips_metro["start_time"] < df_trips_metro["end_time"]
    df_trips_metro = df_trips_metro.loc[start_quality_condition]

    return df_trips_metro


def generate_capital_trips_dataframe(df_capital:pd.DataFrame, df_stations_capital:pd.DataFrame, df_bikes_capital:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset with all valid trips in CapitalBike dataset.

    Parameters
    ----------
    df_capital : pd.DataFrame
        Homogenized CapitalBike DataFrame
    df_stations_capital : pd.DataFrame
        Dataset with all homogenized CapitalBike stations
    df_bikes_capital : pd.DataFrame
        Dataset with all homogenized CapitalBike bikes
    Returns
    -------
    pd.DataFrame
        Dataset with all valid CapitalBike trips
    """    
    trip_columns = [
        'trip_id', 
        'start_time', 
        'end_time',
        'start_station_id',
        'end_station_id',
        'bike_id',
        'member_type',
        'duration'
    ]

    # only trackable trips
    valid_start_stations_mask = df_capital["start_station_id"].isin(df_stations_capital["original_station_id"])
    valid_end_stations_mask = df_capital["end_station_id"].isin(df_stations_capital["original_station_id"])

    df_capital["bike_id"] = df_capital["bike_id"].astype(str)
    valid_bikes_mask = df_capital["bike_id"].isin(df_bikes_capital["original_bike_id"]) 
    condition = valid_start_stations_mask & valid_end_stations_mask & valid_bikes_mask

    df_trips_capital = df_capital.loc[condition, trip_columns].copy()
    df_trips_capital = df_trips_capital.drop_duplicates()

    # change to date time
    df_trips_capital["start_time"] = pd.to_datetime(df_trips_capital["start_time"], format='mixed')
    df_trips_capital["end_time"] = pd.to_datetime(df_trips_capital["end_time"], format='mixed')

    # ensure start time is before end time
    start_quality_condition = df_trips_capital["start_time"] < df_trips_capital["end_time"]
    df_trips_capital = df_trips_capital.loc[start_quality_condition]

    # change stations IDs to int
    df_trips_capital["start_station_id"] = df_trips_capital["start_station_id"].astype(int)
    df_trips_capital["end_station_id"] = df_trips_capital["end_station_id"].astype(int)

    return df_trips_capital


def map_trips_foreign_keys_to_uuids(df_trips:pd.DataFrame, df_stations:pd.DataFrame, df_bikes:pd.DataFrame) -> pd.DataFrame:
    """Map original foreign keys (station IDs, bike IDs) to generated uuids

    Parameters
    ----------
    df_trips : pd.DataFrame
        Dataset with all homogenized trips
    df_stations : pd.DataFrame
        Dataset with all homogenized stations
    df_bikes : pd.DataFrame
        Dataset with all homogenized bikes
    Returns
    -------
    pd.DataFrame
        Dataset containing all homogenized trips with foreign keys mapped to UUIDs
    """    
    # Map values for Stations IDs
    value_map = df_stations.set_index("original_station_id")["station_uuid"].to_dict()
    df_trips["start_station_uuid"] = df_trips["start_station_id"].map(value_map)
    df_trips["end_station_uuid"] = df_trips["end_station_id"].map(value_map)
    df_trips = df_trips.drop(columns=["start_station_id", "end_station_id"])

    # Map values for bike IDs
    value_map = df_bikes.set_index("original_bike_id")["bike_uuid"].to_dict()
    df_trips["bike_uuid"] = df_trips["bike_id"].map(value_map)
    df_trips = df_trips.drop(columns=["bike_id"])

    return df_trips

##########################################################################################
####                                   Demographics                                    ###
##########################################################################################

def generate_demographics_dataframe(df_stations:pd.DataFrame) -> pd.DataFrame:
    """Generate dataset containing demographics information for the stations of both datasets.

    Parameters
    ----------
    df_stations : pd.DataFrame
        Dataset with all stations from both datasets

    Returns
    -------
    pd.DataFrame
        Dataset containing demographics information for the stations
    """    
    variables_dict = {
        "B01001_001E": "population",
        "B01001_002E": "population_male",
        "B01001_026E": "population_female",
        "B01001A_001E": "population_white",
        "B01001B_001E": "population_black",
        "B01001C_001E": "population_american_indian_and_alaska_native",
        "B01001D_001E": "population_asian",
        "B01001E_001E": "population_native_hawaiian_and_other_pacific_islander",
        "B01001F_001E": "population_other_race",
        "B01001G_001E": "population_two_or_more_races",
        "B01001I_001E": "population_hipanic_or_latino",
        "B01002_001E": "median_age",
        "B01002_002E": "median_age_male",
        "B01002_003E": "median_age_female",
        "B06008_002E": "never_married",
        "B06008_003E": "married",
        "B06008_004E": "divorced",
        "B06008_005E": "separated",
        "B06008_006E": "widowed",
        "B06010_002E": "individual_no_income",
        "B06010_003E": "individual_with_income",
        "B08006_002E": "transportation_work_car_truck_van",
        "B08006_008E": "transportation_work_public_transportation",
        "B08006_014E": "transportation_work_bicycle",
        "B08006_015E": "transportation_work_walked",
        "B08006_016E": "transportation_work_taxicab_motorcycle_or_other_means",
        "B08006_017E": "transportation_work_home_office",
        "B19001_002E": "household_income_1_to_9999",
        "B19001_003E": "household_income_10000_to_14999",
        "B19001_004E": "household_income_15000_to_19999",
        "B19001_005E": "household_income_20000_to_24999",
        "B19001_006E": "household_income_25000_to_29999",
        "B19001_007E": "household_income_30000_to_34999",
        "B19001_008E": "household_income_35000_to_39999",
        "B19001_009E": "household_income_40000_to_44999",
        "B19001_010E": "household_income_45000_to_49999",
        "B19001_011E": "household_income_50000_to_59999",
        "B19001_012E": "household_income_60000_to_74999",
        "B19001_013E": "household_income_75000_to_99999",
        "B19001_014E": "household_income_100000_to_124999",
        "B19001_015E": "household_income_125000_to_149999",
        "B19001_016E": "household_income_150000_to_199999",
        "B19001_017E": "household_income_200000_to_more",
        "B19013_001E": "median_household_income",
    }

    variables = ",".join(variables_dict.keys()) 
    year = 2023 # year of the census data
    api_key = os.environ['US_CENSUS_API_KEY'] # need to stablish a valid API key

    zip_codes = df_stations["zip_code"].drop_duplicates().to_list()
    failed_zip_codes = []
    
    df_demographics = pd.DataFrame()
    
    for zip_code in zip_codes:
        # print(zip_code)
        data = request_census_data(zip_code, variables, year, api_key)
    
        # No information in that zip code
        if data is None:
            print(f"No information for zip code: {zip_code}")
            failed_zip_codes.append(zip_code) # handle in following process
            continue
            
        header = data[0][::-1]
        result = data[1][::-1]
        
        df_demographics_row = pd.DataFrame([result], columns=header)
        df_demographics = pd.concat([df_demographics, df_demographics_row])
    
    # change name for interpretability
    df_demographics = df_demographics.rename(columns=variables_dict)
    df_demographics = df_demographics.rename(columns={'zip code tabulation area': 'zip_code'})
    
    # Change to numeric values
    for variable in list(variables_dict.values()) + ["zip_code"]:
        df_demographics[variable] = pd.to_numeric(df_demographics[variable])
    
    # insert mean values for zip codes with no information
    for zip_code in failed_zip_codes:
        df_demographics_row = df_demographics.median().to_frame().T
        df_demographics_row["zip_code"] = zip_code
        df_demographics = pd.concat([df_demographics, df_demographics_row])
    
    df_demographics = df_demographics.reset_index(drop=True)
    
    # add unique identifier
    seed_value = 5
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_demographics))]
    df_demographics.insert(0, 'zip_code_uuid', uuids)

    # impute hidden values
    df_demographics = impute_hidden_values(df_demographics)

    return df_demographics


def impute_hidden_values(df_demographics:pd.DataFrame) -> pd.DataFrame:
    """Impute hidden values of demographics dataset using Iterative Imputer to get more realistic values.

    Parameters
    ----------
    df_demographics : pd.DataFrame
        Dataset containing demographics information with potential hidden values

    Returns
    -------
    pd.DataFrame
        Dataset with imputed hidden values
    """ 
    # start from 2 to avoid using the first two columns: zip_code_uuid and zip_code
    hidden_values_columns = df_demographics.columns[2:][(df_demographics.iloc[:, 2:] < 0).sum(axis=0) > 0]

    for column in hidden_values_columns:
        print(f"Hidden values in column: {column}")
        df_demographics.loc[df_demographics[column] < 0, column] = np.nan

    # no persons living in these zip codes
    zero_values =  df_demographics[df_demographics.iloc[:, 2:].sum(axis=1) == 0 ].fillna(0)
    df_demographics.loc[df_demographics.iloc[:, 2:].sum(axis=1) == 0] = zero_values

    # Create the imputer object with default or chosen parameters
    imputer = IterativeImputer(estimator=BayesianRidge(), max_iter=100, random_state=19)

    # Fit the imputer to the data and transform the data
    df_demographics.iloc[:, 2:] = imputer.fit_transform(df_demographics[df_demographics.columns[2:]])

    return df_demographics


##########################################################################################
####                                      Weather                                      ###
##########################################################################################

def generate_weather_dataframe(raw_data_folder:str) -> pd.DataFrame:
    """Generate dataset containing weather information for Los Angeles and Washington D.C.

    Parameters
    ----------
    raw_data_folder : str
        Folder where raw data files are stored

    Returns
    -------
    pd.DataFrame
        Dataset containing weather information for Los Angeles and Washington D.C.
    """    
    df_weather_los_angeles = pd.read_csv(f"{raw_data_folder}/OpenMeteo/LosAngeles.csv")
    df_weather_los_angeles["city"] = "Los Angeles"

    df_weather_washington = pd.read_csv(f"{raw_data_folder}/OpenMeteo/WashingtonDC.csv")
    df_weather_washington["city"] = "Washington D.C."

    df_weather = pd.concat([df_weather_los_angeles, df_weather_washington])
    df_weather["date"] = df_weather["date"].str.replace(r"[+-]\d{2}:\d{2}", "", regex=True) # change to local time
    df_weather["date"] = pd.to_datetime(df_weather["date"])

    # add unique identifiers
    seed_value = 6
    random.seed(seed_value) # Set a seed for reproducibility

    uuids = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(len(df_weather))]
    df_weather.insert(0, 'weather_uuid', uuids)

    df_weather = df_weather.sort_values(by=["date", "city"])
    df_weather = df_weather.reset_index(drop=True)

    return df_weather


##########################################################################################
####                                       Utils                                       ###
##########################################################################################

def safe_mode(series:pd.Series) -> float:
    """Calculate the mode of a pandas Serie considering empty Series.

    Parameters
    ----------
    series : pd.Series
        Series to calculate the mode

    Returns
    -------
    float
        Mode of the series
    """    
    # Calculate the mode, which returns a Series
    mode_result = series.mode()
    # Check if the result is empty (meaning all values were NaN)
    if mode_result.empty:
        return np.nan
    # Otherwise, return the first mode value
    return mode_result.iloc[0]


def rectify_coordinates(df_stations:pd.DataFrame, city_latitude:float, city_longitude:float, tolerance=3) -> pd.DataFrame:
    """Rectify coordinates that are close to the city but with wrong sign

    Parameters
    ----------
    df_stations : pd.DataFrame
        Dataset with all stations from both datasets
    city_latitude : float
        Latitude of the city center
    city_longitude : float
        Longitude of the city center
    tolerance : int, optional
        Tolerance for considering coordinates close to the city center, by default 3

    Returns
    -------
    pd.DataFrame
        Dataset with rectified coordinates
    """    
    inside_tolerance = (df_stations["lat"].abs() - abs(city_latitude)).abs() <= tolerance
    different_sign = np.sign(df_stations["lat"]) != np.sign(city_latitude)
    
    condition = inside_tolerance & different_sign
    df_stations.loc[condition, "lat"] = df_stations.loc[condition, "lat"] * -1

    inside_tolerance = (df_stations["lon"].abs() - abs(city_longitude)).abs() <= tolerance
    different_sign = np.sign(df_stations["lon"]) != np.sign(city_longitude)
    
    condition = inside_tolerance & different_sign
    df_stations.loc[condition, "lon"] = df_stations.loc[condition, "lon"] * -1

    return df_stations


##########################################################################################
####                                        Main                                       ###
##########################################################################################

def main(argv: List[str] | None = None) -> None:
    """Main function to execute the data cleaning and homogenization pipeline.

    Parameters
    ----------
    argv : List[str] | None, optional
        Command-line arguments, by default None
    """    

    p = argparse.ArgumentParser(description="Data cleaning & homogenization pipeline")
    p.add_argument("--raw-data", default="./RawData", help="Raw data folder (default ./RawData)")
    p.add_argument("--temp-data", default="./TempData", help="Temp data folder (default ./TempData)")
    p.add_argument("--curated-data", default="./CuratedData", help="Curated data folder (default ./CuratedData)")
    p.add_argument("--skip-demographics", action="store_true", help="Skip downloading demographics (requires US_CENSUS_API_KEY)")
    p.add_argument("--skip-weather", action="store_true", help="Skip assembling weather from RawData/OpenMeteo")
    args = p.parse_args(argv)

    raw = args.raw_data
    temp = args.temp_data
    curated = args.curated_data

    ensure_dir(temp)
    ensure_dir(curated)

    print("1) Homogenize datasets (Metro, Capital)")
    df_metro = homogenize_metro_dataset(raw)
    df_capital = homogenize_capital_dataset(raw)

    metro_path = os.path.join(temp, "metro_dataset.csv")
    capital_path = os.path.join(temp, "capital_dataset.csv")
    df_metro.to_csv(metro_path, index=False)
    df_capital.to_csv(capital_path, index=False)
    print(f"Saved homogenized datasets to: {metro_path}, {capital_path}")

    print("2) Generate stations, bikes and trips (unmapped)")
    df_stations = generate_stations_dataframe(df_metro, df_capital)
    df_trips = generate_trips_dataframe(df_metro, df_capital)
    df_bikes = generate_bikes_dataframe(df_metro, df_capital)

    stations_unmapped_path = os.path.join(temp, "stations_unmapped.csv")
    df_stations.to_csv(stations_unmapped_path, index=False)

    trips_unmapped_path = os.path.join(temp, "trips_unmapped.csv")
    df_trips.to_csv(trips_unmapped_path, index=False)

    bikes_path = os.path.join(curated, "bikes.csv") # alrady has all needed information
    df_bikes.to_csv(bikes_path, index=False)

    print(f"Saved unmapped: {stations_unmapped_path}, {bikes_path}, {trips_unmapped_path}")

    if not args.skip_demographics:
        print("3) Generate demographics (will call US Census API). Ensure US_CENSUS_API_KEY is set in environment.")
        if "US_CENSUS_API_KEY" not in os.environ:
            print("US_CENSUS_API_KEY not found in environment; skipping demographics. To enable, set the environment variable and re-run without --skip-demographics.")
        else:
            df_demographics = generate_demographics_dataframe(df_stations)
            demographics_path = os.path.join(curated, "demographics.csv")
            df_demographics.to_csv(demographics_path, index=False)
            print(f"Saved demographics to: {demographics_path}")
    else:
        print("Skipping demographics as requested (--skip-demographics)")

    if not args.skip_weather:
        print("4) Assemble weather from RawData/OpenMeteo -> CuratedData/weather.csv")
        try:
            df_weather = generate_weather_dataframe(raw)
            weather_path = os.path.join(curated, "weather.csv")
            df_weather.to_csv(weather_path, index=False)
            print(f"Saved weather to: {weather_path}")
        except Exception as e:
            print(f"Failed to generate weather dataframe: {e}")
    else:
        print("Skipping weather assembly (--skip-weather)")

    print("5) Map foreign keys to UUIDs (trips, stations)")
    # Map stations foreign keys to demographics UUIDs if demographics were computed
    try:
        df_demographics = None
        demographics_path = os.path.join(curated, "demographics.csv")
        if os.path.exists(demographics_path):
            df_demographics = pd.read_csv(demographics_path)

        if df_demographics is not None:
            df_stations_mapped = map_stations_foreign_keys_to_uuids(df_stations, df_demographics)
        else:
            # If demographics not available, just keep stations
            df_stations_mapped = df_stations.copy()
            print("Station mapping skipped as no demographics exists in Curated data")
        
        stations_path = os.path.join(curated, "stations.csv")
        df_stations_mapped.to_csv(stations_path, index=False)

        # Map trips foreign keys 
        df_trips_mapped = map_trips_foreign_keys_to_uuids(df_trips, df_stations, df_bikes)

        # Also save curated trips partitioned by year and month into CuratedData
        # Filename pattern: CuratedData/trips_{YEAR}_{MM}.csv
        try:
            # ensure start_time is datetime
            df_trips_mapped["start_time"] = pd.to_datetime(df_trips_mapped["start_time"], errors="coerce")
            df_trips_mapped["end_time"] = pd.to_datetime(df_trips_mapped["end_time"], errors="coerce")
            df_trips_valid = df_trips_mapped.dropna(subset=["start_time", "end_time"]).copy()  # don't modify the original

            years = df_trips_valid["start_time"].dt.year
            months = df_trips_valid["start_time"].dt.month

            # Convert time to an standard format
            df_trips_valid["start_time"] = df_trips_valid["start_time"].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
            df_trips_valid["end_time"] = df_trips_valid["end_time"].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

            # groupby using the derived Series rather than adding new columns to the dataframe
            for (y, m), grp in df_trips_valid.groupby([years, months]):
                out_name = os.path.join(curated, f"trips_{y}_{m:02d}.csv")
                grp.to_csv(out_name, index=False)
                print(f"Saved {out_name}")

        except Exception as e:
            print(f"Failed to split and save monthly curated trips: {e}")

    except Exception as e:
        print(f"Failed to map foreign keys: {e}")


if __name__ == "__main__":
    main()