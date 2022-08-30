import os
from asyncio import constants

import xarray as xr


class LocalReader:
    def __init__(self, file_path, constraints):
        self.file_path = file_path
        self.file_size_MB = self.get_file_size_MB()
        self.constraints = constraints

    def get_dataset(self):
        ds = xr.open_dataset(self.file_path)
        if self.constraints is not None:
            ds = ds.isel(self.constraints)
        return ds

    def get_file_size_MB(self):
        return os.path.getsize(self.file_path) / (1024 * 1024)
