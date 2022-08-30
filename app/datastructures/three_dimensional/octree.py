from functools import reduce

import xarray as xr


class Cube:
    def __init__(self, dataset, point_budget, cur_depth, max_depth):
        self.ds = dataset
        self.o_dims = self.get_dims()
        self.point_budget = point_budget
        self.cur_depth = cur_depth
        self.max_depth = max_depth

        self.children = []

        if self.cur_depth != max_depth:
            self.split()

        self.stride_value = self.get_stride_value()

        full_res = get_num_indices(self.ds)
        if "lon" in self.ds.dims:
            self.ds = self.ds.isel(
                lat=slice(None, None, self.stride_value),
                lon=slice(None, None, self.stride_value),
                time=slice(None, None, self.stride_value),
            )
        else:
            self.ds = self.ds.isel(
                latitude=slice(None, None, self.stride_value),
                longitude=slice(None, None, self.stride_value),
                time=slice(None, None, self.stride_value),
            )

        # ((lat_min, lat_max), (lon_min, lon_max), (time_min, time_max))
        self.bounds = self.get_bounds()
        low_res = get_num_indices(self.ds)
        self.resolution = low_res / full_res

    def get_stride_value(self) -> int:
        """
        Returns the stride value for the given dataset and point budget.
        """

        x, y, z = self.get_dims()
        if x * y * z <= self.point_budget:
            return 1

        a = 1
        cur_stride = (x / a) * (y / a) * (z / a)
        while cur_stride > self.point_budget:
            a += 1
            cur_stride = (x / a) * (y / a) * (z / a)

        return a

    def get_dims(self) -> list:
        dims = []
        for axis in self.ds.sizes:
            dims.append(self.ds.sizes[axis])
        return tuple(dims[:3])

    def split(self):
        x_ax = "lon" if "lon" in self.ds.dims else "longitude"
        y_ax = "lat" if "lat" in self.ds.dims else "latitude"
        z_ax = "time" if "time" in self.ds.dims else "TIME"

        mid_x_idx = self.ds.dims[x_ax] // 2
        mid_y_idx = self.ds.dims[y_ax] // 2
        mid_z_idx = self.ds.dims[z_ax] // 2

        if x_ax == "lon":
            c_1 = self.ds.isel(
                lat=slice(mid_y_idx, None),
                lon=slice(None, mid_x_idx),
                time=slice(None, mid_z_idx),
            )
            c_2 = self.ds.isel(
                lat=slice(mid_y_idx, None),
                lon=slice(mid_x_idx, None),
                time=slice(None, mid_z_idx),
            )
            c_3 = self.ds.isel(
                lat=slice(None, mid_y_idx),
                lon=slice(None, mid_x_idx),
                time=slice(None, mid_z_idx),
            )
            c_4 = self.ds.isel(
                lat=slice(None, mid_y_idx),
                lon=slice(mid_x_idx, None),
                time=slice(None, mid_z_idx),
            )
            c_5 = self.ds.isel(
                lat=slice(None, mid_y_idx),
                lon=slice(None, mid_x_idx),
                time=slice(mid_z_idx, None),
            )
            c_6 = self.ds.isel(
                lat=slice(None, mid_y_idx),
                lon=slice(mid_x_idx, None),
                time=slice(mid_z_idx, None),
            )
            c_7 = self.ds.isel(
                lat=slice(mid_y_idx, None),
                lon=slice(None, mid_x_idx),
                time=slice(mid_z_idx, None),
            )
            c_8 = self.ds.isel(
                lat=slice(mid_y_idx, None),
                lon=slice(mid_x_idx, None),
                time=slice(mid_z_idx, None),
            )
        else:
            c_1 = self.ds.isel(
                latitude=slice(mid_y_idx, None),
                longitude=slice(None, mid_x_idx),
                time=slice(None, mid_z_idx),
            )
            c_2 = self.ds.isel(
                latitude=slice(mid_y_idx, None),
                longitude=slice(mid_x_idx, None),
                time=slice(None, mid_z_idx),
            )
            c_3 = self.ds.isel(
                latitude=slice(None, mid_y_idx),
                longitude=slice(None, mid_x_idx),
                time=slice(None, mid_z_idx),
            )
            c_4 = self.ds.isel(
                latitude=slice(None, mid_y_idx),
                longitude=slice(mid_x_idx, None),
                time=slice(None, mid_z_idx),
            )
            c_5 = self.ds.isel(
                latitude=slice(None, mid_y_idx),
                longitude=slice(None, mid_x_idx),
                time=slice(mid_z_idx, None),
            )
            c_6 = self.ds.isel(
                latitude=slice(None, mid_y_idx),
                longitude=slice(mid_x_idx, None),
                time=slice(mid_z_idx, None),
            )
            c_7 = self.ds.isel(
                latitude=slice(mid_y_idx, None),
                longitude=slice(None, mid_x_idx),
                time=slice(mid_z_idx, None),
            )
            c_8 = self.ds.isel(
                latitude=slice(mid_y_idx, None),
                longitude=slice(mid_x_idx, None),
                time=slice(mid_z_idx, None),
            )

        self.children = [
            Cube(c, self.point_budget, self.cur_depth + 1, self.max_depth)
            for c in [c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8]
        ]

    def get_bounds(self):
        if "lon" in self.ds.dims:
            lat_min, lat_max = self.ds.lat.min(), self.ds.lat.max()
            lon_min, lon_max = self.ds.lon.min(), self.ds.lon.max()
            time_min, time_max = self.ds.time.min(), self.ds.time.max()
        else:
            lat_min, lat_max = self.ds.latitude.min(), self.ds.latitude.max()
            lon_min, lon_max = self.ds.longitude.min(), self.ds.longitude.max()
            time_min, time_max = self.ds.time.min(), self.ds.time.max()
        return ((lat_min, lat_max), (lon_min, lon_max), (time_min, time_max))


class Octree:
    def __init__(self, dataset, original_file_size, max_chunk_size):
        self.dataset = dataset
        self.original_file_size = original_file_size
        self.max_chunk_size = max_chunk_size

        self.num_original_points = self.get_num_indices()

        reduction_factor = self.max_chunk_size / self.original_file_size
        self.point_budget = self.num_original_points * reduction_factor

        self.num_layers = self.get_num_layers()
        self.num_chunks_bottomlayer = 8 ** (self.num_layers - 1)

        self.root = self.create_tree()

    def __str__(self) -> str:
        return f"Octree with {self.num_chunks_bottomlayer} chunks of maximum size {self.max_chunk_size}MB"

    def request_data_single_chunk(self, bounds, fit_bounds=False):
        lat_min, lat_max = bounds[0][0], bounds[0][1]
        lon_min, lon_max = bounds[1][0], bounds[1][1]
        time_min, time_max = bounds[2][0], bounds[2][1]

        cur_chunk = self.root

        if not self.overlapping_qubes(bounds, cur_chunk.bounds):
            print(bounds)
            print(cur_chunk.bounds)
            raise Exception("Invalid bounds")

        while len(cur_chunk.children) > 0:
            overlapping_children = []

            for c in cur_chunk.children:
                if self.overlapping_qubes(bounds, c.bounds):
                    overlapping_children.append(c)

            if len(overlapping_children) == 1:
                cur_chunk = overlapping_children[0]
            else:
                break
        if fit_bounds:
            ds = cur_chunk.ds
            ds = ds.sel(
                lat=slice(lat_min, lat_max),
                lon=slice(lon_min, lon_max),
                time=slice(time_min, time_max),
            )
            return (ds, get_bounds(ds), cur_chunk)
        else:
            return (cur_chunk.ds, cur_chunk.bounds, cur_chunk)

    def get_node_resolution(self, cube):
        return cube.resolution

    def request_data(self, bounds, chunk_budget=1, fit_bounds=False):
        lat_min, lat_max = bounds[0][0], bounds[0][1]
        lon_min, lon_max = bounds[1][0], bounds[1][1]
        time_min, time_max = bounds[2][0], bounds[2][1]

        cur_chunk = self.root
        chunks = []

        new_child = []
        while len(cur_chunk.children) > 0:
            for c in cur_chunk.children:
                if self.overlapping_qubes(c.bounds, bounds):
                    new_child.append(c)

            if len(new_child) <= chunk_budget:
                chunks = [i for i in new_child]
                print(chunks)
                cur_chunk = new_child.pop()
            else:
                break

        ds = cur_chunk.ds
        # reshape to requested coords
        # TODO: consider inclusive range
        if fit_bounds:
            ds = ds.sel(
                lat=slice(lat_min, lat_max),
                lon=slice(lon_min, lon_max),
                time=slice(time_min, time_max),
            )

        res = (ds, get_bounds(ds), cur_chunk)

        return res

    def overlapping_qubes(self, query_bounds, chunk_bounds):
        """
        Checks if the query bounds overlap with the chunk bounds.
        """
        lat_min_1, lat_max_1 = query_bounds[0][0], query_bounds[0][1]
        lon_min_1, lon_max_1 = query_bounds[1][0], query_bounds[1][1]
        time_min_1, time_max_1 = query_bounds[2][0], query_bounds[2][1]

        lat_min_2, lat_max_2 = chunk_bounds[0][0], chunk_bounds[0][1]
        lon_min_2, lon_max_2 = chunk_bounds[1][0], chunk_bounds[1][1]
        time_min_2, time_max_2 = chunk_bounds[2][0].values, chunk_bounds[2][1].values

        return (
            lat_min_1 <= lat_max_2
            and lat_min_2 <= lat_max_1
            and lon_min_1 <= lon_max_2
            and lon_min_2 <= lon_max_1
            and time_min_1 <= time_max_2
            and time_min_2 <= time_max_1
        )

    def create_tree(self):
        return Cube(self.dataset, self.point_budget, 1, self.num_layers)

    def get_num_indices(self):
        return reduce(
            (lambda x, y: x * y),
            [self.dataset.sizes.mapping[k] for k in self.dataset.sizes.mapping],
        )

    def get_num_indices_ds(self, ds):
        return reduce(
            (lambda x, y: x * y),
            [ds.sizes.mapping[k] for k in ds.sizes.mapping],
        )

    def get_num_layers(self) -> int:
        i = 0
        cur_size = self.original_file_size / 8**i

        while cur_size > self.max_chunk_size:
            i += 1
            cur_size = self.original_file_size / 8**i

        return i + 1

    def get_initial_dataset(self):
        res = (self.root.ds, self.root.bounds, self.root)
        return res


def get_bounds(ds):
    lat_min, lat_max = ds.lat.min(), ds.lat.max()
    lon_min, lon_max = ds.lon.min(), ds.lon.max()
    time_min, time_max = ds.time.min(), ds.time.max()

    return ((lat_min, lat_max), (lon_min, lon_max), (time_min, time_max))


def get_num_indices(dataset):
    return reduce(
        (lambda x, y: x * y), [dataset.sizes.mapping[k] for k in dataset.sizes.mapping]
    )
