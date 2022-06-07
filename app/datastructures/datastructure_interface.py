from abc import ABC, abstractmethod


class IDatastructure(ABC):
    @abstractmethod
    def request_data_single_chunk(self, bounds):
        pass

    @abstractmethod
    def request_data_n_chunks(self, bounds, chunk_budget, fit_bounds=False):
        pass

    @abstractmethod
    def get_initial_dataset(self):
        pass
