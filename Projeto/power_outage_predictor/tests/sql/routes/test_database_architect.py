import unittest
from sql.routes.database_architect import DatabaseArchitect

class TestDatabaseArchitect(unittest.TestCase):
    def setUp(self):
        self.architect = DatabaseArchitect()
        self.architect.define_project()
    def test_build(self):
        from sql.repositories.database import Database
        self.architect.build()
        self.assertTrue(Database("ingested").exists)
        
if __name__ == '__main__':
    unittest.main()