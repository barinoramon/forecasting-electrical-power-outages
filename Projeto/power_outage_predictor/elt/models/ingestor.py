from abc import ABC, abstractmethod
from sql.repositories.database import Database

class Ingestor(ABC):
    def __init__(self):
        self.source_directory = None
        self.data = None
        self.files_to_be_ingested = None
        self.ingestion_database:Database = Database("ingested")
    
    @abstractmethod
    def _set_files_to_be_ingested(self):
        pass
    @abstractmethod
    def _set_data(self):
        pass
    @abstractmethod
    def _format_data(self):
        pass
    @abstractmethod
    def ingest(self):
        pass
    