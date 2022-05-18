"""
high upfront cost

max chunk size = clients memory limit / 4

"""

from matplotlib import docstring
import xarray as xr
import pandas as pd
import numpy as np


class Chunk:
    def __init__(self, dataframe, layer, tree_depth):
        # sw/ne = (lat, lon)
        self.sw = None
        self.ne = None

        self.ds = dataframe

        self.layer = layer
        self.tree_depth = tree_depth
        self.bounds = self.get_bounds()

        # child nodes
        self.upper_left = None
        self.upper_right = None
        self.lower_left = None
        self.lower_right = None

        # split chunk
        if layer != tree_depth:
            self.split()

        # adjust chunk resolution if above max_chunk_size

    # revisit :(

    def split(self):
        mid_x = (self.ds.shape[1] // 2)
        mid_y = (self.ds.shape[0] // 2)

        c_1 = self.ds[:mid_y, :mid_x]
        c_2 = self.ds[:mid_y, mid_x:]
        c_3 = self.ds[mid_y:, :mid_x]
        c_4 = self.ds[mid_y:, mid_x:]

        self.upper_left = Chunk(c_1, self.layer+1, self.tree_depth)
        self.upper_right = Chunk(c_2, self.layer+1, self.tree_depth)
        self.lower_left = Chunk(c_3, self.layer+1, self.tree_depth)
        self.lower_right = Chunk(c_4, self.layer+1, self.tree_depth)

    def get_bounds(self):
        lon = self.ds.lon.values
        lat = self.ds.lat.values
        return ((lat[0], lon[0]), (lat[-1], lon[-1]))


class QuadTreeSimple:

    def __init__(self, max_chunk_size, original_file_size, dataframe, bounds):

        self.ds = dataframe
        self.bounds = bounds

        # bounds (sw, ne) lat, lon

        self.max_chunk_size = max_chunk_size
        self.original_file_size = original_file_size

        self.num_chunks_bottomlayer = None
        self.num_layers = None
        self.calculate_num_cunks_bottomlayer()

        self.root = self.create_tree()

    def calculate_num_cunks_bottomlayer(self) -> int:
        for i in range(100):
            cells = 4**i
            if self.original_file_size / cells < self.max_chunk_size:
                self.num_layers = i + 1
                self.num_chunks_bottomlayer = cells
                return

    def create_tree(self):
        return Chunk(self.ds, 1, self.num_layers)

    # sw/ne = (lat, lon)

    def get_data(self, sw, ne):
        pass


def main():
    pass


if __name__ == "__main__":
    main()
