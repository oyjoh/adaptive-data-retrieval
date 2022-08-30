from functools import reduce
from webbrowser import get

import xarray as xr

# modified sliding midpoint rule
# prioritied split axis

# dont split at lat, long, time as option

# {'lat':'Strict', 'lon':'Strict', 'depth':'medium'}
# pre defined time slices

# find chunk: traverese utnil only one chunk overlaps


class Node:
    """
    Args:
    custom_rules (list((str, int))): e.g. [('time':10)] indicates that the dataset should be split on the 'time' dimension repeatedly until the nodes have a time range of maximum 10

    Attributes:
        ds (xarray.Dataset): dataset

    """

    def __init__(
        self, ds, point_budget, splits_left, equal_stride, custom_rules=None
    ) -> None:
        self.ds = ds
        self.point_budget = point_budget
        self.splits_left = splits_left
        self.custom_rules = custom_rules
        self.equal_stride = equal_stride

        self.left = None
        self.right = None

        # used to fetch data later
        self.split_dim = None
        self.split_val = None
        if self.splits_left > 0:
            self.split()

        # stride on ds to get to point budget
        full_res = get_num_indices(self.ds)
        self.stride_value = self.get_stride_value()
        slice_ = {d: slice(None, None, self.stride_value) for d in self.ds.dims}
        self.ds = self.ds.isel(**slice_)
        low_res = get_num_indices(self.ds)
        self.resolution = low_res / full_res

    def split(self):
        """
        Splits the dataset into two nodes.
        """
        # find split plane
        idx, dim = self.find_split_plane()
        self.split_dim = dim
        self.split_val = self.ds[dim][idx].values

        # split dataset
        l = self.ds.isel({dim: slice(None, idx)})
        r = self.ds.isel({dim: slice(idx, None)})

        self.left = Node(
            l,
            self.point_budget,
            self.splits_left - 1,
            self.equal_stride,
            self.custom_rules,
        )
        self.right = Node(
            r,
            self.point_budget,
            self.splits_left - 1,
            self.equal_stride,
            self.custom_rules,
        )

    def get_stride_value(self) -> int:
        dims = [self.ds.dims[d] for d in self.ds.dims]

        if self.equal_stride:
            cur_stride = reduce(lambda x, y: x * y, dims)
            a = 1
            while cur_stride > self.point_budget:
                a += 1
                div_dims = [n / a for n in dims]
                cur_stride = reduce(lambda x, y: x * y, div_dims)
            return a
        else:
            # TODO: implement custom stride
            pass

    def find_split_plane(self) -> tuple[str, int]:
        # go by rule
        if self.custom_rules is not None:
            for dim, limit in self.custom_rules:
                if dim in self.ds.dims:
                    if limit < self.ds.dims[dim]:
                        return self.ds.dims[dim] // 2, dim

        # go by largest dim
        dims = []
        for d in self.ds.dims:
            dims.append((self.ds.dims[d], d))

        idx = sorted(dims)[-1][0] // 2
        dim = sorted(dims)[-1][1]

        return idx, dim


class KDTree:
    def __init__(
        self,
        ds: xr.Dataset,
        full_file_size: float,
        max_chunk_size: float,
        custom_rules: list[tuple[str, int]] = None,
        equal_stride=True,
    ) -> None:

        self.ds = ds
        self.custom_rules = custom_rules
        self.full_file_size = full_file_size
        self.max_chunk_size = max_chunk_size
        self.equal_stride = equal_stride

        __num_original_points = self.get_num_indices()
        __reduction_factor = self.max_chunk_size / self.full_file_size
        self.point_budget = __num_original_points * __reduction_factor

        self.num_splits = (
            self.get_num_splits()
        )  # assumes that the splits cuts the dataset into equal parts

        self.num_leafs = 2**self.num_splits

        self.root = self.create_tree()

    def __str__(self) -> str:
        return f"KDTree with {self.num_leafs} leaf nodes of maximum size {self.max_chunk_size}MB"

    def create_tree(self):
        return Node(
            self.ds,
            self.point_budget,
            self.num_splits,
            self.equal_stride,
            self.custom_rules,
        )

    def request_data_single_chunk(self, bounds, fit_bounds=False):
        # bounds = e.g. {'lat':(0,1), 'lon':(0,1), 'depth':(0,1)}
        if type(bounds) is tuple:
            lat_min, lat_max = bounds[0]
            lon_min, lon_max = bounds[1]

            bounds = {"lat": (lat_min, lat_max), "lon": (lon_min, lon_max)}

        cur_chunk = self.root

        while cur_chunk.left is not None and cur_chunk.right is not None:
            # print(f"s dim: {cur_chunk.split_dim}")
            # print(f"bnd {bounds}")
            print(bounds)
            print(cur_chunk.split_dim)
            if cur_chunk.split_dim in bounds:
                if bounds[cur_chunk.split_dim][1] < cur_chunk.split_val:
                    cur_chunk = cur_chunk.left
                elif bounds[cur_chunk.split_dim][0] > cur_chunk.split_val:
                    cur_chunk = cur_chunk.right
                else:
                    break
        return cur_chunk.ds, get_bounds(cur_chunk.ds), cur_chunk

    def get_initial_dataset(self):
        return self.root.ds, get_bounds(self.root.ds), self.root

    def get_num_splits(self):
        i = 0
        cur = self.full_file_size / 2**i

        while cur > self.max_chunk_size:
            i += 1
            cur = self.full_file_size / 2**i

        return i

    def get_num_indices(self):
        return reduce(
            (lambda x, y: x * y),
            [self.ds.sizes.mapping[k] for k in self.ds.sizes.mapping],
        )

    def get_node_resolution(self, node):
        return node.resolution


def get_num_indices(dataset):
    return reduce(
        (lambda x, y: x * y), [dataset.sizes.mapping[k] for k in dataset.sizes.mapping]
    )


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
