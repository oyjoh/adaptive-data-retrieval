import os

from dotenv import load_dotenv
from app.dataprocessing.remote.opendap_access_cas import OpendapAccessCAS
from app.datastructures.three_dimensional.octree import Octree
from app.datastructures.two_dimensional.quad_tree import QuadTree

import xarray as xr
import time


class DataHandler:
    """
    Defines the data source and selects proper data structure
    """

    def __init__(self) -> None:
        self.ds = None  # xarray.Dataset

        self.data_source = None

        self.data_structure = None

        self.max_chunk_size = 50

        self.on_demand_data = False

    def set_max_chunk_size(self, chunk_size):
        self.max_chunk_size = chunk_size

    def set_opendap_cas(self, cas_url, ds_url, username, password, file_size=None):
        self.on_demand_data = True

        if username == None or password == None:
            print('please save credentials to .env')

        self.data_source = OpendapAccessCAS(
            username, password, ds_url, cas_url, file_size_MB=file_size)

        self.ds = self.data_source.get_dataset()
        self.data_structure = self.__set_data_structure()

    def get_data_as_netcfd(self, bounds):
        ds, bounds, node = self.data_structure.request_data(bounds)

        file_name = 'data_' + str(time.time()) + '.nc'  # TODO: revisit.

        ds.to_netcdf(file_name)

        if self.on_demand_data:
            self.__node_stream_to_local_src(node, file_name)

        return file_name

    def get_inital_netcdf(self):
        ds, bounds, node = self.data_structure.get_initial_dataset()

        file_name = 'data_' + str(time.time()) + '.nc'  # TODO: revisit.

        ds.to_netcdf(file_name)
        
        if self.on_demand_data:
            self.__node_stream_to_local_src(node, file_name)


        return file_name
    
    def request_data_netcdf(self, bounds):
        ds, bounds, node = self.data_structure.request_data(bounds, fit_bounds=True, chunk_budget=2)

        file_name = 'data_' + str(time.time()) + '.nc'  # TODO: revisit.

        ds.to_netcdf(file_name)
        
        if self.on_demand_data:
            self.__node_stream_to_local_src(node, file_name)


        return file_name

    def __node_stream_to_local_src(self, node, file_path):
        node.ds = xr.open_dataset(file_path)

    def __set_data_structure(self):
        ds_dims = self.__get_num_dimensions()
        if ds_dims == 2:
            return QuadTree(self.ds, self.data_source.get_file_size_MB(), self.max_chunk_size)
        if ds_dims == 3:
            return Octree(self.ds, self.data_source.get_file_size_MB(), self.max_chunk_size)
        else:
            raise Exception('DataHandler: unsupported number of dimensions')

    def __get_num_dimensions(self):
        return len(self.ds.dims)
