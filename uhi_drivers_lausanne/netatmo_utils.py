"""Netatmo utils."""
import json
from typing import Tuple

import geopandas as gpd
import pandas as pd
import tqdm
from shapely import geometry

NETATMO_CRS = "epsg:4326"
# ECV_DICT = {
#     "precipitation": "rain_live",
#     "pressure": "pressure",
#     "surface_wind_speed": "wind_strength",
#     "surface_wind_direction": "wind_angle",
#     "temperature": "temperature",
#     "water_vapour": "humidity",
# }
variable_col = "variable"
station_id_col = "id"
time_col = "time"
value_col = "value"
geometry_col = "geometry"
variable_columns = [
    "temperature",
    "pressure",
    "wind_strength",
    "wind_angle",
    "rain_live",
]


def get_station_obs_dict(station_record: dict) -> dict:
    """Get a dictionary of observations for a single station record.

    Dictionaries have two metadata keys (i.e., station_id_col and "geometry") and a key
    for each measured variable.

    Parameters
    ----------
    station_record : dict
        Input dictionary of station metadata and observations as returned by the Netatmo
        API.

    Returns
    -------
    obs_dict: dict
        Dictionary of station metadata and observations.
    """
    obs_dict = {
        "id": station_record["_id"],
        "geometry": geometry.Point(*station_record["place"]["location"]),
    }

    # observations = {}
    # NOTE: in the "modules" and "module_types" of station_record, there is the info
    # about the sensors, i.e., NAMain is for pressure (ommited in "module_types"),
    # NAModule1 is for temperature and humidity, NAModule2 is for wind, NAModule3 is for
    # rain. It may be more efficient to use this information to query the
    # station_record["measures"].
    def get_module1_value_dict(record):
        res_dict = record["res"]
        values = res_dict[list(res_dict.keys())[0]]

        return {val: values[i] for i, val in enumerate(record["type"])}

    get_value_dict = {
        "NAMain": lambda record: {"pressure": next(iter(record["res"].values()))[0]},
        "NAModule1": get_module1_value_dict,
        "NAModule2": lambda record: {
            wind_var: record[wind_var] for wind_var in ["wind_strength", "wind_angle"]
        },
        "NAModule3": lambda record: {"rain_live": record["rain_live"]},
    }

    module_types = station_record["module_types"]
    for module_key, module_value_dict in station_record["measures"].items():
        obs_dict.update(
            get_value_dict[module_types.get(module_key, "NAMain")](module_value_dict)
        )

    return obs_dict


def process_response(response_json: dict) -> Tuple[int, gpd.GeoSeries, pd.DataFrame]:
    """Process the response from the Netatmo API.

    Parameters
    ----------
    response_json : dict
        Response from the Netatmo API.

    Returns
    -------
    ts : int
        Server timestamp of the response (Unix time, in seconds).
    station_gser : geopandas.GeoSeries
        Geoseries of station locations, indexed by station ID.
    df : pandas.DataFrame
        Dataframe of station observations, indexed by variable, station ID, and time.
    """
    gdf = gpd.GeoDataFrame(
        [
            get_station_obs_dict(station_record)
            for station_record in response_json["body"]
        ],
        crs=NETATMO_CRS,
    ).set_index(station_id_col)
    return (
        response_json["time_server"],
        gdf[geometry_col],
        gdf.drop(geometry_col, axis=1),  # .transpose(),
    )


def process_filepaths(data_filepaths: list) -> Tuple[pd.DataFrame, gpd.GeoSeries]:
    """Process list of JSON filepaths with Netatmo API responses.

    Parameters
    ----------
    data_filepaths : list of str
        List of JSON filepaths with Netatmo API responses.

    Returns
    -------
    ts_df : pandas.DataFrame
        Dataframe of station observations, indexed by variable, station ID, and time.
    station_gser : geopandas.GeoSeries
        Geoseries of station locations, indexed by station ID.
    """
    ts_df_dict = {}
    station_gser = gpd.GeoSeries(crs=NETATMO_CRS)

    for data_filepath in tqdm.tqdm(data_filepaths):
        with open(data_filepath) as src:
            ts, _gser, _df = process_response(json.load(src))
        station_gser = station_gser.combine_first(_gser)
        # df.loc[pd.IndexSlice(ts, slice(None)), _df.columns] = _df
        ts_df_dict[ts] = _df
    # station_records = response_json["body"]
    ts_df = pd.concat(ts_df_dict.values(), keys=ts_df_dict.keys())

    ts_df = ts_df.stack().rename(value_col)
    ts_df.index = ts_df.index.rename([time_col, station_id_col, variable_col])
    ts_df = ts_df.reset_index().sort_values(by=[variable_col, station_id_col, time_col])
    ts_df[time_col] = pd.to_datetime(ts_df[time_col], unit="s", errors="coerce")
    return ts_df.set_index([variable_col, station_id_col, time_col]), station_gser
