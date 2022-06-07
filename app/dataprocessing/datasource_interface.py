from abc import ABC, abstractmethod


class IDatasource(ABC):
    @abstractmethod
    def get_dataset(self):
        pass
