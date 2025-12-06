"""Data acquisition utilities for the data_curation pipeline.
This module includes functions to unzip bike-share data archives,
download weather data from the Open-Meteo API, and retrieve demographic data from the Census API.
It is designed to be used as part of an end-to-end data curation process.
It can be executed as a standalone script or imported as a module.
"""


import argparse

import pandas as pd
import numpy as np

from uszipcode import SearchEngine
from retry_requests import retry
import openmeteo_requests
import requests_cache
import requests

from SourceCode.utils import unzip_files, ensure_dir

import json
import time
import os


##########################################################################################
####                                     Bike data                                     ###
##########################################################################################

def unzip_archives(raw_data_folder:str, folder_list:list[str]) -> None:
    """Unzip all zip files in the folder list within the raw data folder.

    Parameters
    ----------
    raw_data_folder : str
        Folder where the raw data is stored
    folder_list : list[str]
        List of folder names within the raw data folder to unzip
    """    
    for folder in folder_list:
        data_folder = f"{raw_data_folder}/{folder}"

        zip_file_list = [file for file in os.listdir(data_folder) if file.endswith(".zip")]

        for zip_file in zip_file_list:
            zip_file_path = f"{data_folder}/{zip_file}"
            unzip_files(zip_file_path)


##########################################################################################
####                                      Weather                                      ###
##########################################################################################
def download_openmeteo_for_cities(raw_data_folder:str, start_date:str, end_date:str, sleep_seconds:int=70) -> None:
    """Download the weather data from Open-Meteo for the configured cities and save as CSV files.

    Parameters
    ----------
    raw_data_folder : str
        Folder where the raw data is stored
    start_date : str
        Start date for the weather data in YYYY-MM-DD format
    end_date : str
        End date for the weather data in YYYY-MM-DD format
    sleep_seconds : int, optional
        Number of seconds to sleep between requests, by default 70
    """    
    open_meteo_folder = f"{raw_data_folder}/OpenMeteo"
    ensure_dir(open_meteo_folder)
    
    city_information = {
        "LosAngeles": {
            "coordinates": (34.059753, -118.2375),
            "timezone": "America/Los_Angeles",
        },
        "WashingtonDC": {
            "coordinates": (38.910366, -77.07251),
            "timezone": "America/New_York",
        }
    }

    for city, info in city_information.items():
        print(city)
        latitude, longitude = info["coordinates"]
        df_weather = get_weather_dataframe(latitude, longitude, start_date, end_date)
        
        df_weather["date"] = df_weather["date"].dt.tz_convert(info["timezone"])
        df_weather.set_index("date")
        df_weather.to_csv(f"{open_meteo_folder}/{city}.csv", index=False)
        print("\n" + "----"*15)

        time.sleep(sleep_seconds)


def get_weather_dataframe(latitude:float, longitude:float, start_date:str, end_date:str) -> pd.DataFrame:
    """Get weather data as a DataFrame for a specific location and date range.

    Parameters
    ----------
    latitude : float
        Latitude of the location for which to retrieve weather data
    longitude : float
        Longitude of the location for which to retrieve weather data
    start_date : str
        Start date for the weather data in YYYY-MM-DD format
    end_date : str
        End date for the weather data in YYYY-MM-DD format
    Returns
    -------
    pd.DataFrame
        DataFrame containing the weather data
    """    
    variable_list = [
        "temperature_2m", 
        "relative_humidity_2m", 
        "dew_point_2m", 
        "apparent_temperature", 
        "precipitation", 
        "rain", 
        "snowfall", 
        "snow_depth", 
        "weather_code", 
        "pressure_msl", 
        "surface_pressure", 
        "cloud_cover", 
        "cloud_cover_low", 
        "cloud_cover_mid", 
        "cloud_cover_high", 
        "et0_fao_evapotranspiration", 
        "vapour_pressure_deficit", 
        "wind_speed_10m", 
        "soil_temperature_0_to_7cm", 
        "soil_moisture_0_to_7cm", 
        "soil_temperature_7_to_28cm", 
        "soil_moisture_7_to_28cm", 
        "wind_speed_100m"
    ]

    responses = request_open_meteo(latitude, longitude, start_date, end_date, variable_list)
    
    # Process location
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
    
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    
    # Create DataFrame
    hourly_data = {
        "date": pd.date_range(
        	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        	freq = pd.Timedelta(seconds = hourly.Interval()),
        	inclusive = "left"
        )
    }

    hourly = response.Hourly()
    hourly_data["temperature_2m"] = hourly.Variables(0).ValuesAsNumpy()
    hourly_data["relative_humidity_2m"] = hourly.Variables(1).ValuesAsNumpy()
    hourly_data["dew_point_2m"] = hourly.Variables(2).ValuesAsNumpy()
    hourly_data["apparent_temperature"] = hourly.Variables(3).ValuesAsNumpy()
    hourly_data["precipitation"] = hourly.Variables(4).ValuesAsNumpy()
    hourly_data["rain"] = hourly.Variables(5).ValuesAsNumpy()
    hourly_data["snowfall"] = hourly.Variables(6).ValuesAsNumpy()
    hourly_data["snow_depth"] = hourly.Variables(7).ValuesAsNumpy()
    hourly_data["weather_code"] = hourly.Variables(8).ValuesAsNumpy()
    hourly_data["pressure_msl"] = hourly.Variables(9).ValuesAsNumpy()
    hourly_data["surface_pressure"] = hourly.Variables(10).ValuesAsNumpy()
    hourly_data["cloud_cover"] = hourly.Variables(11).ValuesAsNumpy()
    hourly_data["cloud_cover_low"] = hourly.Variables(12).ValuesAsNumpy()
    hourly_data["cloud_cover_mid"] = hourly.Variables(13).ValuesAsNumpy()
    hourly_data["cloud_cover_high"] = hourly.Variables(14).ValuesAsNumpy()
    hourly_data["et0_fao_evapotranspiration"] = hourly.Variables(15).ValuesAsNumpy()
    hourly_data["vapour_pressure_deficit"] = hourly.Variables(16).ValuesAsNumpy()
    hourly_data["wind_speed_10m"] = hourly.Variables(17).ValuesAsNumpy()
    hourly_data["soil_temperature_0_to_7cm"] = hourly.Variables(18).ValuesAsNumpy()
    hourly_data["soil_moisture_0_to_7cm"] = hourly.Variables(19).ValuesAsNumpy()
    hourly_data["soil_temperature_7_to_28cm"] = hourly.Variables(20).ValuesAsNumpy()
    hourly_data["soil_moisture_7_to_28cm"] = hourly.Variables(21).ValuesAsNumpy()
    hourly_data["wind_speed_100m"] = hourly.Variables(22).ValuesAsNumpy()
    
    df_weather = pd.DataFrame(data = hourly_data)

    return df_weather


def request_open_meteo(latitude:float, longitude:float, start_date:str, end_date:str, variable_list:list) -> list:
    """Get weather data from the Open-Meteo API.

    Parameters
    ----------
    latitude : float
        Latitude of the location for which to retrieve weather data
    longitude : float
        Longitude of the location for which to retrieve weather data
    start_date : str
        Start date for the weather data in YYYY-MM-DD format
    end_date : str
        End date for the weather data in YYYY-MM-DD format
    variable_list : list
        List of weather variables to retrieve

    Returns
    -------
    list
        List of responses from the Open-Meteo API
    """    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
    	"latitude": latitude,
    	"longitude": longitude,
    	"start_date": start_date,
    	"end_date": end_date,
    	"hourly": variable_list,
        "timezone": "auto"
    }
    responses = openmeteo.weather_api(url, params=params)

    return responses


##########################################################################################
####                                   Demographics                                    ###
##########################################################################################

def request_census_data(zip_code:str, variables:str, year:int, api_key:str) -> dict | None:
    """Get demographic data from the Census API.

    Parameters
    ----------
    zip_code : str
        Zip Code of the location for which to retrieve demographic data
    variables : str
        Variables to request from the Census API
    year : int
        Year of the data to retrieve
    api_key : str
        API key for accessing the Census API
    Returns
    -------
    dict | None
        Dictionary containing the requested demographic data or None if an error occurs
    """    
    # Construct the API request URL
    url = f"https://api.census.gov/data/{year}/acs/acs5?get={variables}&for=zip%20code%20tabulation%20area:{zip_code}&key={api_key}"
    
    # Make the request to the API
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError:
        print("Error decoding JSON. Check your API key and URL.")

        return None


def get_zip_code_by_coordenates(df_stations: pd.DataFrame, retries:int=5) -> dict:
    """Get Zip Code from coordenates for all stations in the DataFrame.

    Parameters
    ----------
    df_stations : pd.DataFrame
        DataFrame containing station information with latitude and longitude
    retries : int, optional
        Number of retries for each coordinate lookup, by default 5

    Returns
    -------
    dict
        Dictionary mapping station indices to their corresponding ZIP codes

    Raises
    ------
    RecursionError
        Maximum retries exceeded for a single location
    """    
    zip_dict = {}

    for i, row in enumerate(df_stations.values):
        lat = row[1]
        lon = row[2]

        for retry_i in range(retries):
            try:
                zip_dict[i] = get_zip_code(lat, lon)
                time.sleep(0.001)
                break
            except Exception as e:
                print(f"An error occurred in register {i}: - Error: {e}")
        
        # Raise Exception when max retries exceeded for a single location
        if (retry_i - 1) == retries:
            raise RecursionError(f"Maximum retries exceeded for lat: {lat}, lon: {lon}")

    return zip_dict


def get_zip_code(latitude: float, longitude: float) -> str | float:
    """Get Zip Code from library uszipcode for given latitude and longitude.

    Parameters
    ----------
    latitude : float
        Latitude of the location
    longitude : float
        Longitude of the location

    Returns
    -------
    str | float
        ZIP code as a string if found, otherwise NaN as a float
    """    
    search = SearchEngine(simple_or_comprehensive=SearchEngine.SimpleOrComprehensiveArgEnum.comprehensive)
    result = search.by_coordinates(lat=latitude, lng=longitude, returns=1)
    
    if result:
        zip_code = result[0].zipcode
        # print(f"The ZIP code for coordinates ({latitude}, {longitude}) is: {zip_code}")
    else:
        print(f"No ZIP code found for coordinates ({latitude}, {longitude}).")
        zip_code = np.nan

    return zip_code


##########################################################################################
####                                       Main                                        ###
##########################################################################################

def main(argv: list[str] | None = None) -> None:
    """Main function to run data acquisition steps.

    Parameters
    ----------
    argv : list[str] | None, optional
        List of command-line arguments, by default None
    """    
    p = argparse.ArgumentParser(description="Data acquisition for the bike-share project")
    p.add_argument("--raw-data", dest="raw_data", default="./RawData", help="Raw data folder (default ./RawData)")
    p.add_argument("--start-date", dest="start_date", default="2016-01-01")
    p.add_argument("--end-date", dest="end_date", default="2025-10-01")
    p.add_argument("--no-unzip", dest="no_unzip", action="store_true", help="Skip unzipping archives")
    p.add_argument("--sleep", dest="sleep_seconds", type=int, default=70, help="Seconds to wait between Open-Meteo requests (default 70)")
    args = p.parse_args(argv)

    raw = args.raw_data
    print(f"Using RawData folder: {raw}")

    # 1) Unzip Metro and Capital zip archives
    if not args.no_unzip:
        print("Unzipping Metro and Capital archives...")
        unzip_archives(raw, ["MetroBike", "CapitalBike"])
    else:
        print("Skipping unzip step (--no-unzip set)")

    # 2) Download OpenMeteo data
    print("Downloading OpenMeteo data for configured cities...")
    download_openmeteo_for_cities(raw, args.start_date, args.end_date, sleep_seconds=args.sleep_seconds)


if __name__ == "__main__":
    main()
