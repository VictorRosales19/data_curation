"""Data preparation for predictive analysis.
This module includes functions to prepare data for model training and prediction by consolidating
features from various sources such as trip data, bike information, demographic data, and weather data.
It also defines the columns to be used for modeling.
"""

import pandas as pd

##########################################################################################
####                                  Data preparation                                 ###
##########################################################################################

def prepare_data_for_model(file:str, df_bikes:pd.DataFrame, df_demographics:pd.DataFrame, df_weather:pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare data for model training and prediction by consolidating features from various sources and preparing X and Y datasets.

    Parameters
    ----------
    file : str
        Path file to read the trip data with feature engineering from
    df_bikes : pd.DataFrame
        DataFrame containing bike information
    df_demographics : pd.DataFrame
        DataFrame containing demographic information
    df_weather : pd.DataFrame
        DataFrame containing weather information

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        DataFrames for features (X) and target (Y)
    """    
    # Prepare trips data
    df_trips_feat_month = pd.read_csv(file, parse_dates=True)
    df_trips_feat_month["start_time"] = pd.to_datetime(df_trips_feat_month["start_time"], format="mixed")
    df_trips_feat_month["end_time"] = pd.to_datetime(df_trips_feat_month["end_time"], format="mixed")
    df_trips_feat_month = df_trips_feat_month[trips_columns]
    
    # Add bike type
    df_consolidation = pd.merge(df_trips_feat_month, df_bikes[["bike_uuid", "bike_type"]], on="bike_uuid")
    
    # Add demographics data
    df_consolidation = pd.merge(df_consolidation, df_demographics, left_on="zip_code_uuid_start", right_on="zip_code_uuid")
    
    # Add weather data
    df_consolidation_washington = df_consolidation.loc[df_consolidation["city_start"] == "Washington D.C."]
    df_consolidation_los_angeles = df_consolidation.loc[df_consolidation["city_start"] == "Los Angeles"]
    
    df_weather_washington = df_weather.loc[df_weather["city"]=="Washington D.C."]
    df_weather_los_angeles = df_weather.loc[df_weather["city"]=="Los Angeles"]
    
    df_consolidation_washington = pd.merge_asof(
        df_consolidation_washington, 
        df_weather_washington, 
        left_on="start_time", 
        right_on="date", 
        direction="nearest"
    )
    
    df_consolidation_los_angeles = pd.merge_asof(
        df_consolidation_los_angeles, 
        df_weather_los_angeles, 
        left_on="start_time", 
        right_on="date", 
        direction="nearest"
    )
    
    df_consolidation = pd.concat([df_consolidation_washington, df_consolidation_los_angeles])

    df_X, df_Y = prepare_train_predict_datasets(df_consolidation)
    
    return df_X, df_Y


def prepare_train_predict_datasets(df_consolidation:pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare data for model training and prediction after consolidation

    Parameters
    ----------
    df_consolidation : pd.DataFrame
        Consolidated DataFrame containing features from trips, bikes, demographics, and weather data

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        DataFrames for features (X) and target (Y)
    """    
    df_X = pd.DataFrame()

    for column in columns_dict["variables"]["continuous"]:
        df_X[column] = df_consolidation[column]
    
    for column in columns_dict["variables"]["categorical"]:
        df_X[column] = df_consolidation[column]
        df_X[column] = df_X[column].astype('category') 

        if column == "is_weekend":
            df_X[column] = df_X[column].astype(int)
    
    df_Y = df_consolidation[columns_dict["target"]].copy()

    return df_X, df_Y


def rectify_column_types(df_X:pd.DataFrame) -> pd.DataFrame:
    """Rectify column types for categorical features DataFrame

    Parameters
    ----------
    df_X : pd.DataFrame
        DataFrame containing features for model training or prediction

    Returns
    -------
    pd.DataFrame
        DataFrame with corrected column types
    """    
    for column in columns_dict["variables"]["categorical"]:
        df_X[column] = df_X[column].astype('category') 

        if column == "is_weekend":
            df_X[column] = df_X[column].astype(int)

    return df_X

##########################################################################################
####                                   Columns to use                                  ###
##########################################################################################

# columns to use for the model
columns_dict = {
    "target": 'duration',
    "variables" : {
        "continuous": [
            'latitude_start', 
            'longitude_start',
            'median_household_income',
            'household_income_200000_to_more', 
            'household_income_150000_to_199999',
            'household_income_125000_to_149999',
            'household_income_100000_to_124999', 
            'household_income_75000_to_99999',
            'household_income_60000_to_74999', 
            'household_income_50000_to_59999',
            'household_income_45000_to_49999', 
            'household_income_40000_to_44999',
            'household_income_35000_to_39999', 
            'household_income_30000_to_34999',
            'household_income_25000_to_29999', 
            'household_income_20000_to_24999',
            'household_income_15000_to_19999', 
            'household_income_10000_to_14999',
            'household_income_1_to_9999', 
            'transportation_work_home_office',
            'transportation_work_taxicab_motorcycle_or_other_means',
            'transportation_work_walked', 
            'transportation_work_bicycle',
            'transportation_work_public_transportation',
            'transportation_work_car_truck_van', 
            'individual_with_income',
            'individual_no_income', 
            'widowed', 
            'separated', 
            'divorced', 
            'married',
            'never_married', 
            'median_age_female', 
            'median_age_male', 
            'median_age',
            'population_hipanic_or_latino', 
            'population_two_or_more_races',
            'population_other_race',
            'population_native_hawaiian_and_other_pacific_islander',
            'population_asian', 
            'population_american_indian_and_alaska_native',
            'population_black', 
            'population_white', 
            'population_female',
            'population_male', 
            'population', 
            'temperature_2m', 
            'relative_humidity_2m', 
            'dew_point_2m',
            'apparent_temperature', 
            'precipitation', 
            'rain', 
            'snowfall',
            'snow_depth',  
            'pressure_msl', 
            'surface_pressure',
            'cloud_cover', 
            'cloud_cover_low', 
            'cloud_cover_mid', 
            'cloud_cover_high',
            'et0_fao_evapotranspiration', 
            'vapour_pressure_deficit',
            'wind_speed_10m', 
            'soil_temperature_0_to_7cm', 
            'soil_moisture_0_to_7cm',
            'soil_temperature_7_to_28cm', 
            'soil_moisture_7_to_28cm',
            'wind_speed_100m', 
        ],
        "categorical": [
            'bike_type',
            'member_type',
            'city_start', 
            'hour_of_day',
            'trip_hour_category',
            'weather_code', 
            'day_of_week', 
            'is_weekend', 
            'month',
            'season',
        ]
    }
}

# columns to use for loaded feature engineering trip datasets
trips_columns = [
    'duration',
    'start_time', 
    'member_type', 
    'start_station_uuid', 
    'bike_uuid',
    'station_uuid_start', 
    'latitude_start',
    'longitude_start', 
    'city_start',
    'zip_code_uuid_start', 
    'hour_of_day',
    'trip_hour_category', 
    'day_of_week', 
    'is_weekend', 
    'month', 
    'season',
]