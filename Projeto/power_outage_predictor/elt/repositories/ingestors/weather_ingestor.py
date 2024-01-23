import pandas as pd
from configs.configs import Config
from elt.models.ingestor import Ingestor
from data_lake.directory import Directory
from sql.repositories.database import Database, Table
from elt.repositories.data_formatter import DataFormatter
from general.dataframe_manipulation.dataframe_manipulator import DataframeManipulator


class WeatherIngestor(Ingestor):
    measures_table_name = "weather_measurements"
    stations_table_name = "weather_stations"
    
    measures_remove_columns = [
        'Unnamed: 6'
    ]
    
    stations_remove_columns = [
        "Altitude",
        "Situacao",
        "Data Inicial",
        "Data Final",
        "Periodicidade da Medicao"
    ]
    
    measures_rename_columns = {
        "Codigo Estacao": "codigo_estacao",
        "Data Medicao": "data_medicao",
        "NUMERO DE DIAS COM PRECIP. PLUV, MENSAL (AUT)(número)": "num_dias_com_precipitacao",
        "PRECIPITACAO TOTAL, MENSAL (AUT)(mm)": "precipitacao_total",
        "TEMPERATURA MEDIA, MENSAL (AUT)(°C)": "temperatura_media",
        "VENTO, VELOCIDADE MAXIMA MENSAL (AUT)(m/s)": "velocidade_maxima_vento",
        "VENTO, VELOCIDADE MEDIA MENSAL (AUT)(m/s)": "velocidade_media_vento"
    }
    stations_rename_columns = {
        "Nome": "nome_estacao",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Codigo Estacao": "codigo_estacao"
    }
    
    measures_columns_schema = {"codigo_estacao": str, 
                               "data_medicao": str, 
                               "temperatura_media": float, 
                               "velocidade_maxima_vento": float, 
                               "velocidade_media_vento": float, 
                               "precipitacao_total": float, 
                               "num_dias_com_precipitacao": int}
    stations_columns_schema = {"codigo_estacao": str, 
                               "nome_estacao": str, 
                               "latitude": float, 
                               "longitude": float}
    def __init__(self, directory_path):
        super().__init__()
        self.source_directory = Directory(directory_path)
        self.files_to_be_skipped = self._check_already_ingested_files()
        self.files_to_be_ingested = self._set_files_to_be_ingested()
        self.files_successfully_ingested = []
        self.table_measures = Table(database=self.ingestion_database, name=self.measures_table_name)
        self.table_stations = Table(database=self.ingestion_database, name=self.stations_table_name)
    
    def _set_data(self):
        station_data = pd.DataFrame()
        measure_data = pd.DataFrame()
        
        for file in self.files_to_be_ingested:
            file_station_data, file_measure_data = self._get_file_data(file)
            station_data = station_data._append(file_station_data)
            measure_data = measure_data._append(file_measure_data)
        
        self.data = {}
        self.data["station"] = DataFormatter(station_data, self.stations_rename_columns, self.stations_remove_columns, self.stations_columns_schema) 
        self.data["measure"] = DataFormatter(measure_data, self.measures_rename_columns, self.measures_remove_columns, self.measures_columns_schema)
               
    def _get_file_data(self, file):
        file_station_data = self._get_dataframe_station_data(file.read_csv(header=None, nrows=9, delimiter=';'))
        station_code = file_station_data['Codigo Estacao'].values[0]
        file_measure_data = file.read_csv(skiprows=9, delimiter=';')
        file_measure_data['Codigo Estacao'] = station_code
        return file_station_data, file_measure_data
    
    def _get_dataframe_station_data(self, data):
        dict_data = {}
        for row in range(0,9):
            key, value = data.iloc[row][0].split(":")
            dict_data[key] = value
        
        return pd.DataFrame(dict_data, index=[0])
            
            
    def _format_data(self):
        assert self.data["station"] and self.data["measure"] is not None
        self.data["station"].remove_columns()
        self.data["station"].rename_columns()
        self.data["station"].apply_schema() 
        
        self.data["measure"].remove_columns()
        self.data["measure"].rename_columns()
        self.data["measure"].apply_schema() 
        
    def ingest(self):
        self._set_data()
        self._format_data()
        
        self.ingestion_database.use()
        self.table_measures.insert_dataframe(self.data["measure"].get(), chunksize=None)
        self.table_stations.insert_dataframe(self.data["station"].get(), chunksize=None)

        self._write_ingested_files_file()
        
    
