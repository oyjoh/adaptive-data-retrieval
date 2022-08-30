from abc import ABC, abstractmethod
from functools import reduce
from tokenize import Number

import xarray as xr


class INode(ABC):
    pass


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


def get_ipyleaflet_bounds(
    ds: xr.Dataset,
) -> tuple[tuple[float, float], tuple[float, float]]:
    """
    (SW, NE)
    """
    geo_bounds = get_geo_bounds(ds)
    if "lat" in geo_bounds:
        lat_min, lat_max = geo_bounds["lat"]
    elif "latitude" in geo_bounds:
        lat_min, lat_max = geo_bounds["latitude"]
    if "lon" in geo_bounds:
        lon_min, lon_max = geo_bounds["lon"]
    elif "longitude" in geo_bounds:
        lon_min, lon_max = geo_bounds["longitude"]

    return ((lat_min, lon_min), (lat_max, lon_max))


def spatial_resolution(ds: xr.Dataset) -> str:
    for d in ds.dims:
        if d == "lat" or d == "latitude":
            lat = float(ds[d][1]) - float(ds[d][0])
        if d == "lon" or d == "longitude":
            lon = float(ds[d][1]) - float(ds[d][0])

    return f"{lat:.10f} * {lon:.10f}"
