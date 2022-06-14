import re
from importlib.resources import path
from tkinter import Variable

import xarray as xr


class LocalNetcdfReader:
    def __init__(self, id):
        self.id = id

        datasets = {
            1: "app/externalresources/datasets/GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd",
            2: "app/externalresources/datasets/GEBCO_2021.nc",
        }
        self.path = datasets[self.id]
        # self.ds = self.netcdf_to_dataarray()

    def get_full_dataset(self):
        return self.path

    # (sw, ne) -> ((lat, lon), (lat, lon))
    def get_bounds(self):
        lon = self.ds.lon.values
        lat = self.ds.lat.values
        return ((lat[0], lon[0]), (lat[-1], lon[-1]))

    def netcdf_to_dataarray(self):
        if self.id == 2:
            s = xr.open_dataset(self.path)
            return s["elevation"]
        return xr.open_dataarray(self.path)

    def get_dataarray(self):
        return self.ds

    def get_dataset(self):
        return xr.open_dataset(self.path)
