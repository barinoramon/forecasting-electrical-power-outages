import unittest
from data_lake.data_lake import DataLake
from data_lake.directory import Directory
from data_lake.file import File

class TestDataLake(unittest.TestCase):
    def setUp(self):
        self.test_data_lake = DataLake()
    
    def test_directory_exists(self):
        directory_path = "raw"
        self.assertTrue(self.test_data_lake.directory_exists(directory_path))
    
    def test_file_exists(self):
        file_path = "raw\power_outages\interrupcoes-energia-eletrica-2018.csv"
        self.assertTrue(self.test_data_lake.file_exists(file_path))
    
    def test_get_file(self):
        file_path = "raw\power_outages\interrupcoes-energia-eletrica-2018.csv"
        file = self.test_data_lake.get_file(file_path)
        self.assertTrue(file.path == file_path)
    
    def test_get_directory(self):
        directory_path = "raw"
        directory = self.test_data_lake.get_directory(directory_path)
        self.assertTrue(directory.path == directory_path)
    
    def test_create_directory(self):
        directory = self.test_data_lake.create_directory("test")
        self.assertTrue(self.test_data_lake.directory_exists("test"))
    
    def test_delete_directory(self):
        directory = self.test_data_lake.delete_directory("test")
        self.assertFalse(self.test_data_lake.directory_exists("test"))
        
if __name__ == '__main__':
    unittest.main()