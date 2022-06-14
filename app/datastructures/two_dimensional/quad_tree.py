from functools import reduce
from statistics import mode
from unicodedata import name

import matplotlib.pyplot as plt
import xarray as xr
from sympy import Eq, ceiling, solve, symbols


class Chunk:
    def __init__(self, dataset, layer, tree_depth, point_budget):
        self.ds = dataset
        self.point_budget = point_budget

        self.layer = layer
        self.tree_depth = tree_depth

        # child nodes
        self.upper_left = None
        self.upper_right = None
        self.lower_left = None
        self.lower_right = None

        # split chunk
        if layer != tree_depth:
            self.split()

        self.children = [
            i
            for i in [
                self.upper_left,
                self.upper_right,
                self.lower_left,
                self.lower_right,
            ]
            if i
        ]

        stride_value = get_stride_value(self.ds, point_budget)

        self.ds = self.ds.isel(
            lon=slice(None, None, stride_value), lat=slice(None, None, stride_value)
        )

        # ((lat_min, lat_max), (lon_min, lon_max))
        self.bounds = get_bounds(self.ds)

    def split(self):
        # TODO: define x and y axis
        # TODO: veryfy split without loss, e.g. write tests

        mid_x_idx = self.ds.dims["lon"] // 2
        mid_y_idx = self.ds.dims["lat"] // 2

        c_1 = self.ds.isel(lat=slice(mid_y_idx, None), lon=slice(None, mid_x_idx))
        c_2 = self.ds.isel(lat=slice(mid_y_idx, None), lon=slice(mid_x_idx, None))
        c_3 = self.ds.isel(lat=slice(None, mid_y_idx), lon=slice(None, mid_x_idx))
        c_4 = self.ds.isel(lat=slice(None, mid_y_idx), lon=slice(mid_x_idx, None))

        self.upper_left = Chunk(c_1, self.layer + 1, self.tree_depth, self.point_budget)
        self.upper_right = Chunk(
            c_2, self.layer + 1, self.tree_depth, self.point_budget
        )
        self.lower_left = Chunk(c_3, self.layer + 1, self.tree_depth, self.point_budget)
        self.lower_right = Chunk(
            c_4, self.layer + 1, self.tree_depth, self.point_budget
        )

    def save_netcdf(self, name):
        self.ds.to_netcdf(f"{name}.nc")


class QuadTree:
    def __init__(self, dataset, original_file_size, max_chunk_size):
        self.ds = dataset

        # ((lat_min, lat_max), (lon_min, lon_max))
        # self.bounds = get_bounds(self.ds)

        self.max_chunk_size = max_chunk_size
        self.original_file_size = original_file_size

        self.num_original_points = get_num_indices(self.ds)

        reduction_factor = self.max_chunk_size / self.original_file_size
        self.point_budget = self.num_original_points * reduction_factor

        self.num_layers = self.get_num_layers()
        self.num_chunks_bottomlayer = 4 ** (self.num_layers - 1)

        self.root = self.create_tree()

    def __str__(self) -> str:
        return f"QuadTree with {self.num_chunks_bottomlayer} chunks at lowest level of max chunk size {self.max_chunk_size}"

    def create_tree(self):
        return Chunk(self.ds, 1, self.num_layers, self.point_budget)

    def get_ipyleaflet_bounds(self):
        pass

    def get_num_layers(self):
        i = 0
        cur_size = self.original_file_size / 4**i

        while cur_size > self.max_chunk_size:
            i += 1
            cur_size = self.original_file_size / 4**i

        return i + 1

    """
        def get_num_layers(self) -> int:
        x = symbols("x")
        eq1 = Eq(self.original_file_size / (4**x), self.max_chunk_size)
        sol = solve(eq1)
        return ceiling(sol[-1]) + 1
    """

    def overlapping_bounds(self, b1, b2):
        # ((lat_min, lat_max), (lon_min, lon_max))
        b1_lat, b1_lon = b1
        b2_lat, b2_lon = b2

        if not strict_overlap(b1_lat[0], b1_lat[1], b2_lat[0], b2_lat[1]):
            return False
        if not strict_overlap(b1_lon[0], b1_lon[1], b2_lon[0], b2_lon[1]):
            return False

        return True

    def request_data_m_c(self, bounds, fit_bounds=False):
        print("START REQUEST")
        # TODO: Revisit
        lat_min, lat_max = bounds[0]
        lon_min, lon_max = bounds[1]

        q = [self.root]
        q_layer = [self.root.layer]
        idx = 0

        while True:
            node = q[idx]
            for c in node.children:
                if self.overlapping_bounds(c.bounds, bounds):
                    q.append(c)
                    q_layer.append(c.layer)

            if q_layer.count(q_layer[-1]) >= 4:
                break
            idx += 1
            if idx == len(q):
                break

        budget_layer = 1
        if q_layer.count(q_layer[-1]) > 4:
            budget_layer = q_layer.count(q_layer[-1]) - 1
        else:
            budget_layer = q_layer[-1]

        chunks = []
        print(q_layer)
        for n in q:
            if n.layer == budget_layer:
                chunks.append(n.ds)

        # TODO: seems slow, revisit
        c_chunk = xr.combine_by_coords(chunks)

        # reshape to requested coords
        # TODO: consider inclusive range
        if fit_bounds:
            c_chunk = c_chunk.sel(
                lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
            )

        print("END REQUEST")
        return (c_chunk, get_bounds(c_chunk))

    def request_data(self, bounds, fit_bounds=False):
        # TODO: Revisit
        lat_min, lat_max = bounds[0]
        lon_min, lon_max = bounds[1]

        cur_chunk = self.root

        while len(cur_chunk.children) > 0:
            new_child = []
            for c in cur_chunk.children:
                if self.overlapping_bounds(c.bounds, bounds):
                    new_child.append(c)

            if len(new_child) == 1:
                cur_chunk = new_child[0]
            else:
                break

        ds = cur_chunk.ds
        # reshape to requested coords
        # TODO: consider inclusive range
        if fit_bounds:
            ds = ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))

        res = (ds, get_bounds(ds))

        return res

    def get_initial_dataset(self):
        return self.root.ds, get_bounds(self.root.ds), self.root


def get_bounds(dataset):
    # ((lat_min, lat_max), (lon_min, lon_max))
    lat = (None, None)
    lon = (None, None)

    for dim in dataset.dims:
        d_min = dataset[dim].values[0]
        d_max = dataset[dim].values[-1]

        if dim in ["lat", "latitude"]:
            lat = (d_min, d_max)
        if dim in ["lon", "longitude"]:
            lon = (d_min, d_max)

    return (lat, lon)


def get_num_indices(dataset):
    return reduce(
        (lambda x, y: x * y), [dataset.sizes.mapping[k] for k in dataset.sizes.mapping]
    )


def get_dims(dataset) -> tuple:
    dims = []
    for axis in dataset.sizes:
        dims.append(dataset.sizes[axis])
    return tuple(dims[:2])


def strict_overlap(start1, end1, start2, end2):
    return end1 > start2 and end2 > start1


def get_stride_value(dataset, point_budget) -> int:
    x, y = get_dims(dataset)
    z = symbols("z")
    eq1 = Eq(((x / z) * (y / z)), point_budget)
    sol = ceiling(max(solve(eq1)))
    return sol


def debug():
    ds = xr.open_dataset("app/externalresources/datasets/GEBCO_2021.nc")
    tree = QuadTree(max_chunk_size=50, original_file_size=7470, dataset=ds)

    i_set, _ = tree.request_data(bounds=((-90, -180), (90, 180)))
    print(i_set)
    i_set.to_netcdf("bench.nc")
    # tree.get_initial_dataset().to_netcdf('ii.nc')
    # tree.request_data(((-89, 89), (-179, 179))).to_netcdf('rr.nc')

    # kaland = ((60.2595314, 60.282916), (5.3682603, 5.4547547))
    # tree.request_data(kaland, fit_bounds=True).to_netcdf('kaland_quad.nc')

    # ul = ((0, 90), (0,180))
    # subset = tree.request_data(ul)
    # d_arr = subset['elevation']
    # print('start plot')
    # d_arr.isel().plot()
    # plt.imsave('test.png', arr=d_arr, origin='lower')
    # plt.savefig('res.png')
    # print('end plot')


if __name__ == "__main__":
    debug()
