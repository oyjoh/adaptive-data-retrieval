from importlib.resources import path
from app.externalresources.netcdf_reader_interface import NetcdfReaderInterface

import xarray as xr


class ReaderLokiCastle(NetcdfReaderInterface):
    def __init__(self, id):
        super().__init__(id)
        self.path = 'app/externalresources/datasets/GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd'
        self.ds = None

    def get_full_dataset(self):
        return self.path

    def netcdf_to_dataarray(self):
        ds = xr.open_dataarray(self.path)
        self.ds = ds

    def print_metadata(self):
        print(self.ds)

    def plot_data(self):
        self.ds.isel().plot(robust=True)
