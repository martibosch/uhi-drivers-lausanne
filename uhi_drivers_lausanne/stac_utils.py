"""STAC utils."""
import warnings

import geopandas as gpd
import pystac_client
from shapely import geometry

CLIENT_URL = "https://data.geo.admin.ch/api/stac/v0.9"
# CLIENT_CRS = "EPSG:4326"  # CRS used by the client
CLIENT_CRS = "OGC:CRS84"

SWISSSURFACE3D_RASTER_COLLECTION = "ch.swisstopo.swisssurface3d-raster"
SWISSSURFACE3D_RASTER_CRS = "EPSG:2056"
SWISSSURFACE3D_COLLECTION = "ch.swisstopo.swisssurface3d"
SWISSSURFACE3D_CRS = "EPSG:2056"
SWISSALTI3D_COLLECTION = "ch.swisstopo.swissalti3d"
SWISSALTI3D_CRS = "EPSG:2056"
SWISSALTI3D_NODATA = -9999


class SwissTopoClient:
    """swisstopo client."""

    def __init__(self):
        """Initialize a swisstopo client."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            client = pystac_client.Client.open(CLIENT_URL)
        client.add_conforms_to("ITEM_SEARCH")
        client.add_conforms_to("COLLECTIONS")
        self._client = client

    def gdf_from_collection(
        self,
        collection,
        *,
        extent_geom=None,
        datetime="2019/2019",
        extension=".tif",
        collection_extents_crs=None,
    ):
        """Get geo-data frame of tiles of a collection."""

        def _get_url(item):
            return [
                href for href in item.assets.values() if href.href.endswith(extension)
            ][0].href

        if collection_extents_crs is None:
            collection_extents_crs = self._client.get_collection(
                collection
            ).extra_fields["crs"][0]
        search = self._client.search(
            collections=[collection], intersects=extent_geom, datetime=datetime
        )

        # return pd.DataFrame(
        #     [(get_tif(item), str(item.bbox)) for item in list(search.items())],
        #     columns=[f"{collection}_filepath", "geometry"],
        # )
        return gpd.GeoDataFrame(
            [
                (_get_url(item), geometry.box(*item.bbox))
                for item in list(search.items())
            ],
            crs=collection_extents_crs,
            columns=[collection, "geometry"],
        )
