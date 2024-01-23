import unittest
from sql.routes.database_architect import DatabaseArchitect

class TestDatabaseArchitect(unittest.TestCase):
    def setUp(self):
        self.architect = DatabaseArchitect()
        self.architect.define_project()
    def test_build(self):
        #return
        from sql.repositories.database import Database
        self.architect.build()
        self.assertTrue(Database("ingested").exists)
    def test_destroy(self):
        return
        from sql.repositories.database import Database
        self.architect.destroy()
        self.assertFalse(Database("ingested").exists)
        
if __name__ == '__main__':
    unittest.main()