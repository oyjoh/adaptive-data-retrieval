from re import sub
from matplotlib.transforms import Bbox
from numpy import pad
import xarray as xr
from sympy import solve, symbols, ceiling, Eq
from functools import reduce
import matplotlib.pyplot as plt


class AdaptiveQuery:
    def __init__(self, max_chunk_size, original_file_size, dataset):
        self.ds = dataset
        self.original_file_size = original_file_size
        self.max_chunk_size = max_chunk_size

        self.num_original_points = self.get_num_indices(self.ds)
        reduction_factor = self.max_chunk_size / self.original_file_size
        self.point_budget = self.num_original_points * reduction_factor

    def request_data(self, bounds):
        lat_min, lat_max = bounds[0]
        lon_min, lon_max = bounds[1]

        # slice dataset to fit bounds
        requested_ds = self.ds.sel(lat=slice(lat_min, lat_max),
                                   lon=slice(lon_min, lon_max))

        stride_value = self.get_stride_value(requested_ds, self.point_budget)

        requested_ds = requested_ds.isel(
            lon=slice(None, None, stride_value), lat=slice(None, None, stride_value))

        res = (requested_ds, self.get_bounds(requested_ds))
        return res

    def get_stride_value(self, dataset, point_budget) -> int:
        x, y = self.get_dims(dataset)
        z = symbols('z')
        eq1 = Eq(((x/z)*(y/z)), point_budget)
        sol = ceiling(max(solve(eq1)))
        return(sol)

    def get_dims(self, dataset) -> tuple:
        dims = []
        for axis in dataset.sizes:
            dims.append(dataset.sizes[axis])
        return(tuple(dims[:2]))

    def get_num_indices(self, dataset):
        return reduce(
            (lambda x, y: x * y), [dataset.sizes.mapping[k] for k in dataset.sizes.mapping])

    def get_bounds(self, dataset):
        # ((lat_min, lat_max), (lon_min, lon_max))
        lat = (None, None)
        lon = (None, None)

        for dim in dataset.dims:
            d_min = dataset[dim].values[0]
            d_max = dataset[dim].values[-1]

            if dim in ['lat', 'latitude']:
                lat = (d_min, d_max)
            if dim in ['lon', 'longitude']:
                lon = (d_min, d_max)

        return((lat, lon))


def debug():
    ds = xr.open_dataset('app/externalresources/datasets/GEBCO_2021.nc')
    tree = AdaptiveQuery(max_chunk_size=50, original_file_size=7470,
                         dataset=ds)

    kaland = ((60.2595314, 60.282916), (5.3682603, 5.4547547))
    kaland = ((0, 90), (0, 180))
    # tree.request_data(kaland).to_netcdf('kaland_adaptive.nc')
    subset = tree.request_data(kaland)
    print(subset)
    d_arr = subset['elevation']
    # print(d_arr)
    print(d_arr)

    d_arr.isel().plot()
    plt.imsave('test.png', arr=d_arr, origin='lower')
    plt.savefig('res.png')


if __name__ == '__main__':
    debug()
