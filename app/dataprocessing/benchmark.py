import os
import time

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


class Timer:
    def __init__(self, description="code block"):
        self.description = description

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end = time.perf_counter()
        print(f"Finished '{self.description}' in {self.end - self.start:0.4f} seconds")

class Stopwatch:
    def __init__(self):
        self.description = 'Task'
        self.start_time = None

    def start(self, description='Task'):
        self.description = description
        self.start_time = time.perf_counter()
    
    def stop(self):
        self.end = time.perf_counter()
        return f'{self.description} took {self.end - self.start_time:0.4f} seconds'

class Plot:
    @staticmethod
    def netcdf_color_contour(
        data_variable, ds=None, netcdf_file=None, aspect_ratio=None
    ):
        """
        Plot a contour plot of a netcdf file for selected data variable.
        Every dimesion except for lat,lon will plot at index 0
        """

        if netcdf_file is not None:
            ds = xr.open_dataset(netcdf_file)

        ds[data_variable].isel(
            {x: 0 for x in ds.dims if x not in ["lat", "lon", "latitude", "longitude"]}
        ).plot()

        if aspect_ratio is not None:
            ax = plt.gca()
            ax.set_aspect(aspect_ratio)

        plt.show()

    @staticmethod
    def netcdf_color_contour_multi_time(
        data_variable, time, ds=None, netcdf_file=None
    ):

        if netcdf_file is not None:
            ds = xr.open_dataset(netcdf_file)

        args = {x: 0 for x in ds.dims if x not in ["lat", "lon", "time"]}

        n_time = len(time)
        row, col = 2, 2
        if n_time == 2:
            row = 1
            col = 2
        elif n_time == 4:
            row = 2
            col = 2
        else:
            raise Exception("Invalid number of plots")

        for idx, t in enumerate(time):
            plt.subplot(row, col, idx + 1)
            ds[data_variable].isel(args).sel(time=t).plot()
        plt.show()


def get_file_size_MB(file_path):
    file_size_bytes = os.path.getsize(file_path)
    file_size_MB = file_size_bytes / (1024 * 1024)

    return file_size_MB


def plt_img(arr, vmin, vmax):
    # plt.imsave(f'dashboard_render{id}.png', arr=ds, origin='lower')
    # file_name = "data_" + str(time.time())[-5:] + ".nc"  # TODO: revisit.

    file_name = "img_" + str(time.time())[-5:] + ".png"

    plt.imsave(fname=file_name, arr=arr, origin="lower", vmin=vmin, vmax=vmax)

    return file_name
