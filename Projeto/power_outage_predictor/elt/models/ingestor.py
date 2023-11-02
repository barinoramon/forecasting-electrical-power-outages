from abc import ABC, abstractmethod
from sql.repositories.database import Database
from data_lake.file import File

class Ingestor(ABC):
    ingested_files_file_path = "ingested_files.txt"
    def __init__(self):
        self.source_directory = None
        self.data = None
        self.files_to_be_ingested = None
        self.files_to_be_skipped = None
        self.ingestion_database:Database = Database("ingested")
    
    def _write_ingested_files_file(self):
        with open(self.ingested_files_file_path, 'a') as ingested_file:
            for file_name in self.files_successfully_ingested:
                ingested_file.write(file_name + "\n")
    
    def _set_files_to_be_ingested(self):
        file_list = self.source_directory.list_files()
        new_files = [file for file in file_list if file not in self.files_to_be_skipped]
        return [File(file) for file in new_files]
    
    def _check_already_ingested_files(self):
        if self.source_directory.file_exists(self.ingested_files_file_path):
             with open(ingested_files_file_path, 'r') as ingested_file:
                return [file_name.strip() for file_name in ingested_files_file_path.readlines()]
        else:
            return []
    
    @abstractmethod
    def _set_data(self):
        pass
    @abstractmethod
    def _format_data(self):
        pass
    @abstractmethod
    def ingest(self):
        pass
    