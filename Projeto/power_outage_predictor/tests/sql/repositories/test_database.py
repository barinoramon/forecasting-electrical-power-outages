import unittest
from sql.repositories.database import Database

class TestDatabase(unittest.TestCase):  
    def setUp(self):
        self.test_database = Database('test')
        
    def test_database_exists(self):
        non_existing_database = self.test_database
        self.assertFalse(non_existing_database.exists)
        existing_database = Database('sys')
        self.assertTrue(existing_database.exists)
        
    def test_create_database(self):
        self.test_database.create()
        self.assertTrue(self.test_database.exists)
    
    def test_delete_database(self):
        self.test_database.delete()
        self.assertFalse(self.test_database.exists)
    
    def test_get_existing_tables_list(self):
        self.assertEqual(self.test_database.get_existing_tables_list(), [])
        
    def tearDown(self):
        self.test_database.delete()

if __name__ == '__main__':
    unittest.main()