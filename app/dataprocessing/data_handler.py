import datetime
import os
import time

import xarray as xr
from app.dataprocessing.benchmark import Timer
from app.dataprocessing.datasource_interface import IDatasource
from app.dataprocessing.local.local_reader import LocalReader
from app.dataprocessing.remote.opendap_access_cas import OpendapAccessCAS
from app.datastructures.datastructure_interface import INode, IStructure, get_bounds
from app.datastructures.n_dimensional.kd_tree import KDTree
from app.datastructures.three_dimensional.octree import Octree
from app.datastructures.two_dimensional.quad_tree import QuadTree
from dotenv import load_dotenv
from sympy import im


class CachedDS:
    def __init__(self, node: INode):
        self.ds = node.ds
        self.bounds = get_bounds(node.ds)
        self.time_stamp = datetime.datetime.now()
        self.resolution = node.resolution

    def __str__(self) -> str:
        return f"\tBounds:{self.bounds}\n\tCreated:{self.time_stamp}\n\tResolution:{self.resolution:.2f}"


class DataHandler:
    """
    Defines the data source and selects proper data structure
    """

    def __init__(self) -> None:
        self.ds = None  # xarray.Dataset

        self.data_source: IDatasource = None

        self.data_structure: IStructure = None

        self.max_chunk_size = 50

        self.on_demand_data = False

        self.custom_rules = None

        self.cache: list[CachedDS] = []

    def set_max_chunk_size(self, chunk_size):
        self.max_chunk_size = chunk_size

    def get_cache(self):
        return self.cache

    def set_custom_rules(self, custom_rules):
        self.custom_rules = custom_rules

    def set_opendap_cas(
        self,
        cas_url,
        ds_url,
        username,
        password,
        file_size=None,
        constraints=None,
        struct=None,
    ):
        self.on_demand_data = True

        if username == None or password == None:
            print("please save credentials to .env")

        self.data_source = OpendapAccessCAS(
            username,
            password,
            ds_url,
            cas_url,
            file_size_MB=file_size,
            constraints=constraints,
        )

        self.ds = self.data_source.get_dataset()
        with Timer("Creating data structure"):
            self.data_structure = self.__set_data_structure(struct)

    def set_local_netcdf_reader(self, file_path, constraints=None, struct=None):
        self.data_source = LocalReader(file_path, constraints)

        with Timer("Loading dataset"):
            self.ds = self.data_source.get_dataset()
        with Timer("Creating data structure"):
            self.data_structure = self.__set_data_structure(struct)

    def get_inital_netcdf(self):
        ds, bounds, node = self.data_structure.get_initial_dataset()

        file_name = "tmp/nc/data_" + str(time.time()) + ".nc"  # TODO: revisit.

        ds.to_netcdf(file_name)

        if self.on_demand_data:
            self.__node_stream_to_local_src(node, file_name)

        return file_name

    def get_initial_ds(self):
        ds, bounds, node = self.data_structure.get_initial_dataset()

        return ds, bounds, node

    def request_data_netcdf(self, bounds, return_xr_chunk=False, fit_bounds=False):
        ds, bounds, node = self.data_structure.request_data_single_chunk(
            bounds, fit_bounds=fit_bounds
        )

        file_name = "tmp/nc/data_" + str(time.time())[-5:] + ".nc"  # TODO: revisit.

        ds.to_netcdf(file_name)

        if self.on_demand_data and fit_bounds == False:
            self.__node_stream_to_local_src(node, file_name)

        if return_xr_chunk:
            return file_name, bounds, node
        else:
            return file_name

    def get_file_size_MB(self, file_path):
        return os.path.getsize(file_path) / (1024 * 1024)

    def get_node_resolution(self, node):
        return self.data_structure.get_node_resolution(node) * 100

    def get_node_spatial_resolution(self, node) -> dict:
        return self.data_structure.get_node_spatial_resolution(node)

    def get_full_xr_ds(self) -> xr.Dataset:
        return self.data_structure.ds

    def __node_stream_to_local_src(self, node, file_path):
        # store cache in list

        node.ds = xr.open_dataset(file_path)
        self.cache.append(CachedDS(node))

    def __set_data_structure(self, custom):
        if custom:
            if custom == "KDTree":
                return KDTree(
                    self.ds,
                    full_file_size=self.data_source.get_file_size_MB(),
                    max_chunk_size=self.max_chunk_size,
                    custom_rules=self.custom_rules,
                )

        ds_dims = self.__get_num_dimensions()
        if ds_dims == 2:
            return QuadTree(
                self.ds, self.data_source.get_file_size_MB(), self.max_chunk_size
            )
        elif ds_dims == 3:
            return Octree(
                self.ds, self.data_source.get_file_size_MB(), self.max_chunk_size
            )
        elif ds_dims > 3:
            return KDTree(
                self.ds,
                full_file_size=self.data_source.get_file_size_MB(),
                max_chunk_size=self.max_chunk_size,
                custom_rules=self.custom_rules,
            )
        else:
            raise Exception("DataHandler: unsupported number of dimensions")

    def __get_num_dimensions(self):
        return len(self.ds.dims)
