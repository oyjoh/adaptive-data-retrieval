# set up conection to a dataset available by OPeNDAP
# build for Copernicus data

import xarray as xr
from pydap.cas.get_cookies import setup_session
from pydap.client import open_url
import math

from dotenv import load_dotenv
import os

# TODO: handle age when caching data


class OpendapAccessCAS:
    def __init__(self, username, password, dataset_url, cas_url='https://cmems-cas.cls.fr/cas/login', file_size_MB=None):
        self.username = username
        self.password = password
        self.dataset_url = dataset_url
        self.cas_url = cas_url

        self.ds = self.copernicusmarine_datastore()

        self.file_size_MB = self.estimate_file_size(
        ) if file_size_MB is None else file_size_MB

    def get_dataset(self):
        return self.ds

    def copernicusmarine_datastore(self):
        session = setup_session(
            self.cas_url, username=self.username, password=self.password)
        session.cookies.set('CASTGC', session.cookies.get_dict()['CASTGC'])

        data_store = xr.backends.PydapDataStore(
            open_url(self.dataset_url, session=session))
        ds = xr.open_dataset(data_store)

        return ds
    
    


    # TODO: not robust, not smart
    def estimate_file_size(self):
        self.ds.isel(time=0).to_netcdf('slice.nc')
        file_size_bytes = os.path.getsize('slice.nc')
        file_size_MB = file_size_bytes/(1024*1024)

        time_dim = self.ds.dims['time']

        estimate_MB = time_dim*file_size_MB

        return int(math.ceil(estimate_MB))

    def get_file_size_MB(self):
        return self.file_size_MB


def debug():
    load_dotenv()

    USERNAME = os.environ.get('CMEMS_CAS_USERNAME')
    PASSWORD = os.environ.get('CMEMS_CAS_PASSWORD')

    if USERNAME == None or PASSWORD == None:
        print('please save credentials to .env')

    cas_url = 'https://cmems-cas.cls.fr/cas/login'

    url = 'https://nrt.cmems-du.eu/thredds/dodsC/METEOFRANCE-EUR-SST-L4-NRT-OBS_FULL_TIME_SERIE'

    o_reader = OpendapAccessCAS(
        USERNAME, PASSWORD, url, cas_url, file_size_MB=500)

    print(o_reader.ds)


if __name__ == '__main__':
    debug()
