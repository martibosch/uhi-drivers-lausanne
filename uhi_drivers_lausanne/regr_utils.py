"""Regression features."""

import geopandas as gpd
import pandas as pd


def get_station_features_gdf(
    stations_gdf_filepath: str,
    features_filepaths: list,
    *,
    fillna: float | bool = 0,
) -> gpd.GeoDataFrame:
    """Get station features geo-data frame.

    Parameters
    ----------
    stations_gdf_filepath : str
        Path to stations geo-data frame.
    features_filepaths : list of str
        List of paths to features data frames, with a common index column with the
        stations geo-data frame.
    fillna : float or bool, default 0
        Value to replace NaNs with, by default 0. A value of False will not replace
        NaNs.

    Returns
    -------
    station_features_gdf : gpd.GeoDataFrame
        Station features geo-data frame.

    """
    station_features_gdf = gpd.read_file(stations_gdf_filepath)
    station_index_name = station_features_gdf.columns.drop("geometry")[0]
    # stations_gdf = stations_gdf.set_index(station_index_name)
    station_features_gdf = pd.concat(
        [
            station_features_gdf.set_index(station_index_name),
        ]
        + [
            pd.read_csv(features_filepath).set_index(station_index_name)
            for features_filepath in features_filepaths
        ],
        axis="columns",
    )
    if fillna is not False:
        station_features_gdf = station_features_gdf.fillna(fillna)
    return station_features_gdf


def _read_and_resample(
    ts_df_filepath: str,
    *,
    start_dt: str | None = None,
    end_dt: str | None = None,
) -> pd.DataFrame:
    """Read time series data frame and resample to hourly frequency.

    Parameters
    ----------
    ts_df_filepath : str
        Path to time series data frame.
    start_dt : str, optional
        Start date and time, by default None (does not filter data)
    end_dt : str, optional
        End date and time, by default None (does not filter data)


    Returns
    -------
    ts_df : pd.DataFrame
        Time series data frame resampled to hourly frequency.

    """
    ts_df = pd.read_csv(ts_df_filepath)
    ts_df["time"] = pd.to_datetime(ts_df["time"])
    ts_df = ts_df.set_index("time").resample("H").mean()
    if start_dt is not None:
        ts_df = ts_df.loc[start_dt:]
    if end_dt is not None:
        ts_df = ts_df.loc[:end_dt]
    return ts_df


def get_long_ts_df(
    ts_df_filepaths: list[str],
    station_index_name: str,
    *,
    start_dt: str | None = None,
    end_dt: str | None = None,
) -> pd.DataFrame:
    """Get time series data frame in long format.

    Parameters
    ----------
    ts_df_filepaths : list of str
        Paths to time series data frame (in wide format).
    station_index_name : str
        Name of the column in the time series data frame that contains the station
        index.
    start_dt : str, optional
        Start date and time, by default None (does not filter data)
    end_dt : str, optional
        End date and time, by default None (does not filter data)

    Returns
    -------
    ts_df : pd.DataFrame
        Time series data frame in long format.

    """
    #
    return pd.concat(
        [
            _read_and_resample(ts_df_filepath, start_dt=start_dt, end_dt=end_dt)
            .stack(dropna=True)
            .rename_axis(["time", station_index_name])
            .reset_index(name="T")
            for ts_df_filepath in ts_df_filepaths
        ],
        axis="rows",
    )


def get_uhi_df(ts_df: pd.DataFrame) -> pd.DataFrame:
    """Get urban heat island (UHI) data frame.

    Obtain the UHI magnitude at each station and timestamp by subtracting the minimum
    temperature at each timestamp from the temperature at each station and timestamp.

    Parameters
    ----------
    ts_df : pd.DataFrame
        Time series data frame in long format.

    Returns
    -------
    uhi_df : pd.DataFrame
        Urban heat island data frame.

    """
    return (
        ts_df.drop("time", axis="columns")
        .groupby(ts_df["time"])
        .apply(
            lambda group_df: group_df.assign(
                **{"UHI": group_df["T"] - group_df["T"].min()}
            ).drop("T", axis="columns")
        )
        .droplevel(-1)
        .reset_index()
    )
