from data_lake.directory import Directory
from data_lake.file import File
from general.dataframe_manipulation.dataframe_manipulator import DataframeManipulator
from elt.repositories.data_formatter import DataFormatter
import pandas as pd
from sql.repositories.database import Database, Table
from elt.models.ingestor import Ingestor

class PowerOutagesIngestor(Ingestor):
    remove_columns = ['DatGeracaoConjuntoDados', 'DscAlimentadorSubestacao', 
                      'DscSubestacaoDistribuicao', 'NumOrdemInterrupcao', 
                      'IdeMotivoInterrupcao',
                      'NumNivelTensao', 'NumUnidadeConsumidora', 
                      'NumAno', 'NomAgenteRegulado', 'SigAgente', 'NumCPFCNPJ']
    rename_columns = {"DscConjuntoUnidadeConsumidora": "conjunto_eletrico", 
                      "DscTipoInterrupcao": "tipo_interrupcao", 
                      "DatInicioInterrupcao": "data_inicio", 
                      "DatFimInterrupcao": "data_fim", 
                      "DscFatoGeradorInterrupcao": "causa", 
                      "NumConsumidorConjunto": "n_consumidor_conjunto"}
    columns_schema = {"conjunto_eletrico": str, "tipo_interrupcao": str, "data_inicio": str, "data_fim": str, "causa": str, "n_consumidor_conjunto": str}
    table_name = "power_outages"
    ingested_files_file_path = "ingested_files.txt"
    def __init__(self, directory_path):
        super().__init__()
        self.source_directory = Directory(directory_path)
        self.files_to_be_skipped = self._check_already_ingested_files()
        self.files_to_be_ingested = self._set_files_to_be_ingested()
        self.files_successfully_ingested = []
        self.table = Table(database=self.ingestion_database, name=self.table_name)
        
    def _write_ingested_files_file(self):
        with open(ingested_files_file_path, 'a') as ingested_file:
            for file_name in self.files_successfully_ingested:
                ingested_file.write(file_name + "\n")
        
    
    def _check_already_ingested_files(self):
        if self.source_directory.file_exists(self.ingested_files_file_path):
             with open(ingested_files_file_path, 'r') as ingested_file:
                return [file_name.strip() for file_name in ingested_files_file_path.readlines()]
        else:
            return []
        
    def _set_files_to_be_ingested(self):
        file_list = self.source_directory.list_files()
        new_files = [file for file in file_list if file not in self.files_to_be_skipped]
        return [File(file) for file in new_files]
    
    def _set_data(self):
        files_data = pd.DataFrame()
        
        for file in self.files_to_be_ingested:
            file_data = file.read()
            files_data = pd.concat([files_data, file_data])
            self.files_successfully_ingested.append(file.name)
            
        self.data = DataFormatter(files_data, self.rename_columns, self.remove_columns, self.columns_schema)
            
    def _format_data(self):
        assert self.data is not None
        self.data.remove_columns()
        self.data.rename_columns()
        self.data.apply_schema()
    
    def ingest(self):
        self._set_data()
        self._format_data()
        
        self.ingestion_database.use()
        self.table.insert_dataframe(self.data.get())

        self._write_ingested_files_file()
        
        
        
                