import xarray as xr
import pandas as pd
import numpy as np

class Chunk:
    def __init__(self):
        pass


class QuadTreeDrop:

    def __init__(self, max_chunk_size, original_file_size, dataframe):
        self.max_chunk_size = max_chunk_size
        self.ds = dataframe
        self.original_file_size = original_file_size

        self.num_chunks_bottomlayer = None
        self.num_layers = None
        self.calculate_num_cunks_bottomlayer()

        self.root = Chunk()

    def calculate_num_cunks_bottomlayer(self) -> int:
        for i in range(100):
            cells = 4**i
            if self.original_file_size / cells < self.max_chunk_size:
                self.num_layers = i + 1
                self.num_chunks_bottomlayer = cells
                return

    # sw/ne = (lat, lon)

    def get_data(self, sw, ne):
        pass


def main():
    ds = xr.DataArray(np.random.randint(1, 10, size=(10, 10)))

    qt = QuadTreeDrop(10, 24, [])
    print(qt.num_chunks_bottomlayer)


if __name__ == "__main__":
    main()
