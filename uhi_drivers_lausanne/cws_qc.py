"""Quality control for CWS data.

Based on Napoly et al., 2018 (https://doi.org/10.3389/feart.2018.00118)
"""

import matplotlib as mpl
import pandas as pd
import seaborn as sns
from scipy.stats import norm
from statsmodels.robust import scale

from uhi_drivers_lausanne import settings

# def get_mislocated_stations(station_gser: gpd.GeoSeries) -> pd.Series:
#     """Get mislocated stations.

#     When multiple stations share the same location, it is likely due to an incorrect
#     set up that led to automatic location assignment based on the IP address of the
#     wireless network.

#     Parameters
#     ----------
#     station_gser : geopandas.GeoSeries
#         Geoseries of station locations (points).

#     Returns
#     -------
#     mislocated_stations : pandas.Series
#         Boolean series indicating whether a station (index) is mislocated (indicated
#         by a value of `True`).
#     """
#     return station_gser.duplicated(keep=False)


def get_outlier_stations(
    ts_df: pd.DataFrame,
    *,
    low_alpha: float | None = None,
    high_alpha: float | None = None,
    station_outlier_threshold: float | None = None,
) -> pd.Series:
    """Get outlier stations.

    Measurements can show suspicious deviations from a normal distribution (based on
    a modified z-score using robust Qn variance estimators). Stations with high
    proportion of such measurements can be related to radiative errors in non-shaded
    areas or other measurement errors.

    Parameters
    ----------
    ts_df : pandas.DataFrame
        Time series of measurements (rows) for each station (columns).
    low_alpha, high_alpha : numeric, optional
        Values for the lower and upper tail respectively (in proportion from 0 to 1)
        that lead to the rejection of the null hypothesis (i.e., the corresponding
        measurement does not follow a normal distribution can be considered an
        outlier). If None, the respective values from `settings.OUTLIER_LOW_ALPHA`
        and `settings.OUTLIER_HIGH_ALPHA` are used.
    station_outlier_threshold : numeric, optonal
        Maximum proportion (from 0 to 1) of outlier measurements after which the
        respective station may be flagged as faulty. If None, the value from
        `settings.STATION_OUTLIER_THRESHOLD` is used.

    Returns
    -------
    outlier_stations : pandas.Series
        Boolean series indicating whether a station (index) is considered an outlier
        (indicated by a value of `True`).
    """

    def z_score(x):
        return (x - x.median()) / scale.qn_scale(x.dropna())

    if low_alpha is None:
        low_alpha = settings.OUTLIER_LOW_ALPHA
    if high_alpha is None:
        high_alpha = settings.OUTLIER_HIGH_ALPHA
    if station_outlier_threshold is None:
        station_outlier_threshold = settings.STATION_OUTLIER_THRESHOLD
    # ts_df = pd.DataFrame(self.ts_gdf.drop("geometry", axis=1).T)
    nonnan_df = ~ts_df.isna()
    low_z = norm.ppf(low_alpha)
    high_z = norm.ppf(high_alpha)
    outlier_df = (
        ~ts_df.apply(z_score, axis=1).apply(
            lambda z: z.between(low_z, high_z, inclusive="neither"), axis=1
        )
        & nonnan_df
    )
    prop_outlier_ser = outlier_df.sum() / nonnan_df.sum()

    return prop_outlier_ser > station_outlier_threshold


def get_indoor_stations(
    ts_df: pd.DataFrame, *, station_indoor_corr_threshold: float | None = None
) -> pd.Series:
    """Get indoor stations.

    Stations whose time series of measurements show low correlations with the
    spatial median time series are likely set up indoors.

    Parameters
    ----------
    ts_df : pandas.DataFrame
        Time series of measurements (rows) for each station (columns).
    station_indoor_corr_threshold : numeric, optonal
        Stations showing Pearson correlations (with the overall station median
        distribution) lower than this threshold are likely set up indoors. If None,
        the value from `settings.STATION_INDOOR_CORR_THRESHOLD` is used.

    Returns
    -------
    indoor_stations : pandas.Series
        Boolean series indicating whether a station (index) is likely set up indoors
        (indicated by a value of `True`).
    """
    if station_indoor_corr_threshold is None:
        station_indoor_corr_threshold = settings.STATION_INDOOR_CORR_THRESHOLD

    return ts_df.corrwith(ts_df.median(axis=1)) < station_indoor_corr_threshold


# plotting
# def _get_cws_official_ts_df(cws_ts_df, official_ts_df, cws_label, official_label):
#     return pd.concat(
#         [
#             ts_df.reset_index()
#             .melt(id_vars="time", var_name="station_id", value_name="T")
#             .assign(**{"source": source})
#             for source, ts_df in zip(
#                 [cws_label, official_label], [cws_ts_df, official_ts_df]
#             )
#         ],
#         ignore_index=True,
#     )


def comparison_lineplot(
    cws_ts_df: pd.DataFrame,
    official_ts_df: pd.DataFrame,
    cws_label: str = "netatmo",
    official_label: str = "official",
    value_label: str = "T",
    station_label: str = "station_id",
    source_label: str = "source",
    **lineplot_kws,
) -> mpl.axes.Axes:
    """Lineplot comparing CWS and official stations time series.

    Parameters
    ----------
    cws_ts_df, official_ts_df : pandas.DataFrame
        Time series of measurements (rows) for each station (columns).
    cws_label, official_label : str, optional
        Labels for the CWS and official stations respectively, by default "netatmo"
        and "official".
    value_label : str, optional
        Label for the values, by default "T" (for temperature).
    station_label : str, optional
        Label for the stations, by default "station_id".
    source_label : str, optional
        Label for the source, by default "source".
    **lineplot_kws
        Keyword arguments to pass to `seaborn.lineplot`.

    Returns
    -------
    matplotlib.axes.Axes
        Axes of the plot.
    """
    return sns.lineplot(
        data=pd.concat(
            [
                ts_df.reset_index()
                .melt(id_vars="time", var_name=station_label, value_name=value_label)
                .assign(**{source_label: source})
                for source, ts_df in zip(
                    [cws_label, official_label], [cws_ts_df, official_ts_df]
                )
            ],
            ignore_index=True,
        ),
        x="time",
        y=value_label,
        hue=source_label,
        **lineplot_kws,
    )
