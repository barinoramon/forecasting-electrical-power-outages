import unittest
from elt.load.ingestor_pipeline import IngestorPipeline

class TestIngestorPipeline(unittest.TestCase):
    def test_ingestor_pipeline(self):
        ingestor_pipeline = IngestorPipeline()
        ingestor_pipeline.ingest_power_outages_data()
        
if __name__ == '__main__':
    unittest.main()