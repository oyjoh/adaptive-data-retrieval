import os

import xarray as xr


class LocalReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_size_MB = self.get_file_size_MB()

    def get_dataset(self):
        return xr.open_dataset(self.file_path)

    def get_file_size_MB(self):
        return os.path.getsize(self.file_path) / (1024 * 1024)
