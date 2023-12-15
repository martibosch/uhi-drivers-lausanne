"""Official stations utils."""
import tempfile
from os import environ, path

import boto3
import geopandas as gpd
import pandas as pd


class SpacesClient:
    """Client to get weather data stored in Spaces (DigitalOcean)."""

    def __init__(
        self,
        bucket_name: str,
        *,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """Initialize the client.

        Parameters
        ----------
        bucket_name : str
            Name of the bucket.
        access_key_id : str, optional
            Access key ID. If no value is provided, the value set in the
            `AWS_ACCESS_KEY_ID` environment variable will be used.
        secret_access_key : str, optional
            Secret access key. If no value is provided, the value set in the
            `AWS_SECRET_ACCESS_KEY` environment variable will be used.
        region_name : str, optional
            The region to use when creating the session. If no value is provided,
            the value set in the `AWS_REGION` environment variable will be used.
        profile_name : str, optional
            The profile to use when creating your session. If no value is provided, the
            value set in the `AWS_PROFILE` environment variable will be used.
        endpoint_url : str, optional
            The complete URL to use for the constructed client. If no value is provided,
            the value set in the `S3_ENDPOINT_URL` environment variable will be used.
        """
        # # load dotenv file
        # _ = dotenv.load_dotenv(dotenv.find_dotenv())
        # check if variables are provided, otherwise get them from the environment
        if access_key_id is None:
            access_key_id = environ.get("AWS_ACCESS_KEY_ID")
        if secret_access_key is None:
            secret_access_key = environ.get("AWS_SECRET_ACCESS_KEY")
        if region_name is None:
            region_name = environ.get("AWS_REGION")
        if profile_name is None:
            profile_name = environ.get("AWS_PROFILE")
        if endpoint_url is None:
            # if using DigitalOcean Spaces instead of AWS S3
            endpoint_url = environ.get("S3_ENDPOINT_URL")
        self.session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
            profile_name=profile_name,
        )
        self.client = self.session.client(
            "s3",
            endpoint_url=endpoint_url,
        )
        self.bucket_name = bucket_name

    def get_idaweb_df(self, key: str) -> pd.DataFrame:
        """Get IDAWEB data frame.

        Parameters
        ----------
        key : str
            Key of the object in the bucket.

        Returns
        -------
        idaweb_df : pandas.DataFrame
            IDAWEB data frame.
        """
        idaweb_df = pd.read_csv(
            self.client.get_object(Bucket=self.bucket_name, Key=key).get("Body"),
            sep=";",
            na_values="-",
        )
        # drop rows that correspond to station names in the txt file
        idaweb_df = idaweb_df.drop(idaweb_df[idaweb_df["stn"] == "stn"].index)
        # pivot, ensure numeric and set datetime index
        idaweb_df = idaweb_df.pivot(
            index="time",
            columns="stn",
            values=idaweb_df.columns.drop(["stn", "time"])[0],
        ).apply(pd.to_numeric)
        idaweb_df.index = pd.to_datetime(idaweb_df.index)

        return idaweb_df

    def get_vaudair_df(self, key: str) -> pd.DataFrame:
        """Get Vaud'air data frame.

        Parameters
        ----------
        key : str
            Key of the object in the bucket.

        Returns
        -------
        vaudair_df : pandas.DataFrame
            Vaudair data frame.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_filepath = path.join(tmp_dir, path.basename(key))
            with open(tmp_filepath, "wb") as tmp_file:
                self.client.download_fileobj(self.bucket_name, key, tmp_file)
            vaudair_df = (
                pd.read_excel(tmp_filepath, index_col=0)
                .iloc[2:]
                .apply(pd.to_numeric, axis=1)
            )
        vaudair_df.index = pd.to_datetime(vaudair_df.index.rename("time"))

        return vaudair_df


def get_station_gser(
    station_location_filepath: str,
    station_location_crs: str,
    x_col: str = "x",
    y_col: str = "y",
    station_col: str = "stn",
) -> gpd.GeoSeries:
    """Get a geoseries of station locations.

    Parameters
    ----------
    station_location_filepath : str
        Path to the station locations file.
    station_location_crs : str
        CRS of the station locations.
    x_col : str, optional
        Name of the column containing the x coordinates, by default "x".
    y_col : str, optional
        Name of the column containing the y coordinates, by default "y".
    station_col : str, optional
        Name of the column containing the station names, by default "stn".

    Returns
    -------
    station_gser : geopandas.GeoSeries
        Geoseries of station locations, in the CRS specified by `station_location_crs`.
    """
    station_location_df = pd.read_csv(station_location_filepath)
    station_gser = gpd.GeoSeries(
        gpd.points_from_xy(station_location_df[x_col], station_location_df[y_col]),
        index=station_location_df[station_col],
        crs=station_location_crs,
    )
    return station_gser
