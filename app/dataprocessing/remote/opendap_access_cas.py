import math
import os

import xarray as xr
from dotenv import load_dotenv
from pydap.cas.get_cookies import setup_session
from pydap.client import open_url


class OpendapAccessCAS:
    """
    Set up conection to a dataset available by OPeNDAP
    Copernicus data

    Args:
        username, password (str): CAS SSO credentials for the OPeNDAP server
        cas_url (str): URL of the CAS server
        file_size_MB (float): dataset file size in MB, if not provided, will be estimated

    Attributes:
        ds (xarray.Dataset): OPeNDAP connection to the datasource
        file_size_MB (float)
    """

    def __init__(
        self,
        username,
        password,
        dataset_url,
        cas_url="https://cmems-cas.cls.fr/cas/login",
        file_size_MB=None,
    ):
        self.username = username
        self.password = password
        self.dataset_url = dataset_url
        self.cas_url = cas_url

        self.ds: xr.Dataset = self.copernicusmarine_datastore()

        self.file_size_MB = (
            self.estimate_file_size() if file_size_MB is None else file_size_MB
        )

    def get_dataset(self):
        return self.ds

    def copernicusmarine_datastore(self):
        session = setup_session(
            self.cas_url, username=self.username, password=self.password
        )
        session.cookies.set("CASTGC", session.cookies.get_dict()["CASTGC"])

        data_store = xr.backends.PydapDataStore(
            open_url(self.dataset_url, session=session)
        )
        ds = xr.open_dataset(data_store)

        return ds

    def estimate_file_size(self):
        """
        Estimates the file size of the dataset
        Creates a netcdf file with a small part of the dataset and uses it to estiamte the total file size

        Returns:
            float: file size in MB
        """

        slice_dim = (
            "time" if "time" in self.ds.dims else list(self.ds.dims.mapping.keys())[0]
        )

        self.ds.isel({slice_dim: 0}).to_netcdf("tmp.nc")
        file_size_bytes = os.path.getsize("tmp.nc")
        os.remove("tmp.nc")
        file_size_MB = file_size_bytes / (1024 * 1024)

        time_dim = self.ds.dims[slice_dim]

        estimate_MB = time_dim * file_size_MB

        return int(math.ceil(estimate_MB))

    def get_file_size_MB(self):
        return self.file_size_MB
