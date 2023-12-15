"""Plotting utils."""
import contextily as cx
import geopandas as gpd
import matplotlib as mpl
import pandas as pd


def plot_stations_by_var(
    station_gser: gpd.GeoSeries,
    var_ser: pd.Series,
    *,
    legend: bool = True,
    edgecolor: str = "black",
    attribution: str | bool = False,
    set_axis_off: bool = True,
    plot_kws: dict | None = None,
    add_basemap_kws: dict | None = None,
) -> mpl.axes.Axes:
    """Plot stations by variable.

    Parameters
    ----------
    station_gser : gpd.GeoSeries
        GeoSeries of stations.
    var_ser : pd.Series
        Series of variable values.
    legend : bool, optional
        Whether to add a legend, by default True.
    edgecolor : str, optional
        Color of the edges, by default "black".
    attribution : str | bool, optional
        Attribution of the basemap, by default False.
    set_axis_off : bool, optional
        Whether to set the axis off, by default True.
    plot_kws : dict, optional
        Keyword arguments to pass to `geopardas.GeoDataFrame.plot`, by default None.
    add_basemap_kws : dict, optional
        Keyword arguments to pass to `contextily.add_basemap`, by default None.

    Returns
    -------
    matplotlib.axes.Axes
        Axes of the plot.
    """
    if plot_kws is None:
        plot_kws = {}
    if add_basemap_kws is None:
        add_basemap_kws = {}
    name = getattr(var_ser, "name", "var")
    ax = gpd.GeoDataFrame({name: var_ser}, geometry=station_gser).plot(
        name, legend=legend, edgecolor=edgecolor, **plot_kws
    )
    cx.add_basemap(ax, crs=station_gser.crs, attribution=attribution, **add_basemap_kws)
    if set_axis_off:
        ax.set_axis_off()
    return ax
