"""Canopy utils."""
import glob
import io
import tempfile
import zipfile
from os import path
from urllib import request

import detectree as dtr
import numpy as np
import pdal
from rasterio import transform


def get_lidar_arrays(lidar_url, gser, lidar_values, dst_res):
    """Get lidar arrays."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(io.BytesIO(request.urlopen(lidar_url).read())) as z:
            # we know that there is only one file in the zip, i.e., the .las file
            lidar_filepath = z.extract(z.namelist()[0], tmp_dir)
        pipeline = (
            pdal.Reader(lidar_filepath)
            | pdal.Filter.expression(
                expression=" || ".join(
                    [f"Classification == {value}" for value in lidar_values]
                )
            )
            # | pdal.Filter.crop(polygon=geom.wkt)
            # | pdal.Writer.gdal(
            #     raster_filepath,
            #     resolution=TARGET_CANOPY_RES,
            #     nodata=0,
            #     output_type="count",
            #     origin_x=minx,
            #     origin_y=miny,
            #     width=width,
            #     height=height,
            #     default_srs=COLLECTION_DATA_CRS,
            # )
            | pdal.Filter.crop(polygon=[geom.wkt for geom in gser])
            | pdal.Writer(path.join(tmp_dir, "geom-#.las"))
        )
        _ = pipeline.execute()
        arrays = []
        transforms = []
        for _lidar_filepath, geom in zip(
            sorted(glob.glob(path.join(tmp_dir, "geom-*.las"))), gser
        ):
            west, south, east, north = geom.bounds
            # # it is probably better to use `ceil` to make sure that we span the max
            # # possible extent, but we cannot exceed the maximum width/height allocated
            # # in the data array
            # width = int(min(np.ceil((east - west) / dst_res), max_width))
            # height = int(min(np.ceil((north - south) / dst_res), max_height))
            width = int(np.round((east - west) / dst_res))
            height = int(np.round((north - south) / dst_res))
            t = transform.from_bounds(west, south, east, north, width, height)
            arrays.append(
                dtr.rasterize_lidar(
                    _lidar_filepath,
                    lidar_values,
                    (height, width),
                    t,
                )
            )
            transforms.append(t)

    return arrays, transforms
