from app.externalresources.netcdf_reader_interface import NetcdfReaderInterface


class ReaderLokiCastle(NetcdfReaderInterface):
    def __init__(self, id):
        super().__init__(id)
        self.path = 'app/externalresources/datasets/GS17_ROV01_LokiCastle_08072017_LLD_depthcorrected.grd'

    def get_full_dataset(self):
        return self.path
