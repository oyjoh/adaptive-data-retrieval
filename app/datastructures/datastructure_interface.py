from abc import ABC, abstractmethod
from functools import reduce
from tokenize import Number

import xarray as xr


class IStructure(ABC):
    @abstractmethod
    def request_data_single_chunk(self, bounds):
        pass

    @abstractmethod
    def request_data_n_chunks(self, bounds, chunk_budget, fit_bounds=False):
        pass

    @abstractmethod
    def get_initial_dataset(self):
        pass


class INode(ABC):
    pass


def get_num_indices_ds(ds: xr.Dataset) -> int:
    return reduce(
        (lambda x, y: x * y),
        [ds.sizes.mapping[k] for k in ds.sizes.mapping],
    )


def get_geo_bounds(ds: xr.Dataset) -> dict[str, float]:
    bounds = get_bounds(ds)
    geo_bounds = {}
    for b in bounds:
        if b == "lat" or b == "latitude":
            geo_bounds[b] = (float(bounds[b][0]), float(bounds[b][1]))
        elif b == "lon" or b == "longitude":
            geo_bounds[b] = (float(bounds[b][0]), float(bounds[b][1]))
    return geo_bounds


def get_bounds(ds: xr.Dataset) -> dict:
    return {d: (ds[d].values.min(), ds[d].values.max()) for d in ds.dims}
