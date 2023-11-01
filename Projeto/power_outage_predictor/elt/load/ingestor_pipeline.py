import sys
from elt.models.pipeline import Pipeline
from elt.repositories.power_outages_ingestor import PowerOutagesIngestor
from data_lake.data_lake import DataLake

class IngestorPipeline(Pipeline):
    def __init__(self):
        super().__init__()
        self.data_lake = DataLake()
        self.data_source = {
            'power_outages': self.data_lake.get_directory("raw\power_outages")
        }
    def ingest_power_outages_data(self):
        #try:
            power_outages_ingestor = PowerOutagesIngestor(self.data_source['power_outages'].path)
            power_outages_ingestor.ingest()
        #except Exception as e:
         #   print(f'Failed to ingest power outages data: {e}', file=sys.stderr, flush=True, end='')
    def script(self):
        self.ingest_power_outages_data()
            
            