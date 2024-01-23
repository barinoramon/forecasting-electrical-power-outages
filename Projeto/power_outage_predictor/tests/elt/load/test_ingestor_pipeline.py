import unittest
from elt.load.ingestor_pipeline import IngestorPipeline

class TestIngestorPipeline(unittest.TestCase):
    def setUp(self):
        self.ingestor_pipeline = IngestorPipeline()
    def test_ingest_power_outages(self):
        self.ingestor_pipeline.ingest_power_outages_data()
    def test_ingest_consumer_units(self):
        self.ingestor_pipeline.ingest_consumer_units_data()
    def test_ingest_weather(self):
        self.ingestor_pipeline.ingest_weather_data()
        
if __name__ == '__main__':
    unittest.main()