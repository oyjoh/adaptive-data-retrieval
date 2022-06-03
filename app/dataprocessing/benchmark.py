import time
import matplotlib.pyplot as plt
from pyrsistent import CheckedValueTypeError
import xarray as xr
import os


class Timer:
    def __init__(self, description='code block'):
        self.description = description

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end = time.perf_counter()
        print(
            f'Finished \'{self.description}\' in {self.end - self.start:0.4f} seconds')


class Plot:
    def __init__(self):
        pass

    @staticmethod
    def netcdf_color_contour(data_variable, ds=None, netcdf_file=None):
        """
        Plot a contour plot of a netcdf file for selected data variable.
        Every dimesion except for lat,lon will plot at index 0
        """

        if netcdf_file is not None:
            ds = xr.open_dataset(netcdf_file)

        ds[data_variable].isel(
            {x: 0 for x in ds.dims if x not in ['lat', 'lon']}).plot()
        plt.show()


class Benchmark:
    def __init__(self):
        pass

    @staticmethod
    def get_file_size_MB(file_path):
        file_size_bytes = os.path.getsize(file_path)
        file_size_MB = file_size_bytes/(1024*1024)

        return file_size_MB
