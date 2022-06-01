from importlib.resources import path
from app.externalresources.netcdf_reader_interface import NetcdfReaderInterface

import xarray as xr


class ReaderLokiCastle(NetcdfReaderInterface):
    def __init__(self, id=None):
        super().__init__(id=1)
        self.path = 'app/externalresources/datasets/GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd'
        self.ds = None

    def get_full_dataset(self):
        return self.path

    # sw, ne, (lat, lon)
    def get_bounds(self):
        lon = self.ds.lon.values
        lat = self.ds.lat.values

        return ((lat[0], lon[0]), (lat[-1], lon[-1]))

    def netcdf_to_dataarray(self):
        ds = xr.open_dataarray(self.path)
        self.ds = ds

    def print_metadata(self):
        print(self.ds)

    def plot_data(self):
        self.ds.isel().plot(robust=True)

    def get_dataarray(self):
        return(self.ds)
