import pandas as pd
from configs.configs import Config
from elt.models.ingestor import Ingestor
from data_lake.directory import Directory
from sql.repositories.database import Database, Table
from elt.repositories.data_formatter import DataFormatter
from general.dataframe_manipulation.dataframe_manipulator import DataframeManipulator


class ConsumerUnitsIngestor(Ingestor):
    table_name = "consumer_units"
    remove_columns = ['DatGeracaoConjuntoDados', 'SigAgente', 'NumCNPJ']
    rename_columns = {'IdeConjUndConsumidoras': 'id_conjunto_eletrico', 
                               'DscConjUndConsumidoras':'conjunto_eletrico', 
                               'SigIndicador':'indicador', 
                               'AnoIndice':'ano', 
                               'NumPeriodoIndice':'mes',
                               'VlrIndiceEnviado':'valor'
    }
    columns_schema = {"id_conjunto_eletrico": int, "conjunto_eletrico": str, "indicador": str, "ano": str, "mes": int, "valor": float}
    def __init__(self, directory_path):
        super().__init__()
        self.source_directory = Directory(directory_path)
        self.files_to_be_skipped = self._check_already_ingested_files()
        self.files_to_be_ingested = self._set_files_to_be_ingested()
        self.files_successfully_ingested = []
        self.table = Table(database=self.ingestion_database, name=self.table_name)
        self.indicators = self.config.assets["ingestion"]["indicators"]
    
    def _set_data(self):
        files_data = pd.DataFrame()
        
        for file in self.files_to_be_ingested:
            file_data = file.read_csv(delimiter=';', encoding='utf-8', encoding_errors='replace')
            files_data = pd.concat([files_data, file_data])
            self.files_successfully_ingested.append(file.name)
            
        self.data = DataFormatter(files_data, self.rename_columns, self.remove_columns, self.columns_schema)
        
    def _format_data(self):
        assert self.data is not None
        self.data.remove_columns()
        self.data.rename_columns()
        self.data.apply_schema() 
        self.data.data.filter_rows("indicador", self.indicators)
        
    def ingest(self):
        self._set_data()
        self._format_data()
        
        self.ingestion_database.use()
        self.table.insert_dataframe(self.data.get(), chunksize=10000)

        self._write_ingested_files_file()
        
    
