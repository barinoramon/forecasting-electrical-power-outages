import unittest
import pandas as pd
from sql.repositories.database import Database


class TestTable(unittest.TestCase):
    def setUp(self):
        self.database = Database('test')
        if not self.database.exists:
            self.database.create()
        
        self.database.use()
            
        self.table = self.database.get_table('test_table')
        self.table_data = pd.DataFrame({'a': ["a", "b", "c"], 'b': [int(2), int(3), int(1)], 'c': [2.5,2.4,2.3]})
        self.columns_settings = [('a', 'varchar(1)'), ('b', 'int'), ('c', 'decimal')]
    
    def test_create_table(self):
        self.table.create(self.columns_settings)
        self.assertTrue(self.table.exists)
        
    def test_insert_dataframe(self):
        if not self.table.exists:
            self.table.create(self.columns_settings)
        self.table.insert_dataframe(self.table_data)
        self.assertEqual(self.table.get_table_length(), len(self.table_data))
        self.table.display()
        
    def test_delete_table(self):
        self.table.delete()
        self.assertFalse(self.table.exists)
    
    def tearDown(self):
        self.table.delete()
        self.database.delete()

if __name__ == '__main__':
    unittest.main()
