import os
import time

import xarray as xr
from app.dataprocessing.benchmark import Timer
from app.dataprocessing.local.local_reader import LocalReader
from app.dataprocessing.remote.opendap_access_cas import OpendapAccessCAS
from app.datastructures.datastructure_interface import IDatastructure
from app.datastructures.n_dimensional.kd_tree import KDTree
from app.datastructures.three_dimensional.octree import Octree
from app.datastructures.two_dimensional.quad_tree import QuadTree
from dotenv import load_dotenv


class DataHandler:
    """
    Defines the data source and selects proper data structure
    """

    def __init__(self) -> None:
        self.ds = None  # xarray.Dataset

        self.data_source = None

        self.data_structure: IDatastructure = None

        self.max_chunk_size = 50

        self.on_demand_data = False

        self.custom_rules = None

    def set_max_chunk_size(self, chunk_size):
        self.max_chunk_size = chunk_size

    def set_custom_rules(self, custom_rules):
        self.custom_rules = custom_rules

    def set_opendap_cas(self, cas_url, ds_url, username, password, file_size=None):
        self.on_demand_data = True

        if username == None or password == None:
            print("please save credentials to .env")

        self.data_source = OpendapAccessCAS(
            username, password, ds_url, cas_url, file_size_MB=file_size
        )

        self.ds = self.data_source.get_dataset()
        self.data_structure = self.__set_data_structure()

    def set_local_netcdf_reader(self, file_path):
        self.data_source = LocalReader(file_path)

        with Timer("Loading dataset"):
            self.ds = self.data_source.get_dataset()
        with Timer("Creating data structure"):
            self.data_structure = self.__set_data_structure()

    def get_inital_netcdf(self):
        ds, bounds, node = self.data_structure.get_initial_dataset()

        file_name = "data_" + str(time.time()) + ".nc"  # TODO: revisit.

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

        file_name = "data_" + str(time.time())[-5:] + ".nc"  # TODO: revisit.

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

    def __node_stream_to_local_src(self, node, file_path):
        node.ds = xr.open_dataset(file_path)

    def __set_data_structure(self):
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
                ds=self.ds,
                full_file_size=self.data_source.get_file_size_MB(),
                max_chunk_size=self.max_chunk_size,
                custom_rules=self.custom_rules,
            )
        else:
            raise Exception("DataHandler: unsupported number of dimensions")

    def __get_num_dimensions(self):
        return len(self.ds.dims)
