from app.externalresources.reader_LokiCastle import ReaderLokiCastle


class Datagenerator():
    def __init__(self, id, constraints) -> None:
        self.id = id
        self.constraints = constraints

        id_to_reader = {1: ReaderLokiCastle}

        self.reader = id_to_reader[1](id)

    def generate_dataset(self, file_format='NetCDf', fullres=False):
        if fullres:
            return self.reader.get_full_dataset()
        else:
            pass
