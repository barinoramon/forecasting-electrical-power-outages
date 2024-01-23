from general.singleton.singleton import SingletonMeta
from sql.repositories.database import Database, Table

class DatabaseConstructor(metaclass=SingletonMeta):
    def __init__(self):
        self.databases_structure = None
    
    def _verify_database_structure(self,databases_strocture:dict):
        for key, value in databases_strocture.items():
            assert isinstance(key, str), "Keys must be string"
            assert isinstance(value, list), "Values must be list"
            assert all(isinstance(item, dict) for item in value), "Values list must be a list of dictionaries"
            
    def _create_database(self, database_name:str):
        database = Database(database_name)
        if not database.exists:
            database.create()
        return database.exists
    
    def _delete_database(self, database_name:str):
        database = Database(database_name)
        if database.exists:
            database.delete()
    
    def _create_table(self, database_name:str, table_setting:dict):
        database = Database(database_name)
        database.use()
        table = Table(database=database, name=table_setting["name"])
        if not table.exists:
            table.create(table_setting["columns"])
        return table.exists
    
    def _delete_table(self, database_name:str, table_name:str):
        database = Database(database_name)
        database.use()
        table = Table(database=database, name=table_name)
        if table.exists:
            table.delete()
    
    def _create_database_tables(self, database_name:str, tables_settings:dict):
        for table_setting in tables_settings:
            table_was_created = self._create_table(database_name, table_setting)
            if table_was_created:
                print(f'Table {table_setting["name"]} successfully created.')
            else:
                print(f'Failed to create Table {table_setting["name"]}.')
    
    def set_architecture(self, databases_structure:dict):
        self._verify_database_structure(databases_structure)
        self.databases_structure = databases_structure
    
    def build(self):
        for database_name, tables_settings in self.databases_structure.items():
            database_was_created = self._create_database(database_name)
            if database_was_created:
                print(f'Database {database_name} successfully created.')
                self._create_database_tables(database_name, tables_settings)
            else:
                print(f'Failed to create database {database_name}.')
                
    def destroy(self):
        for database_name,_ in self.databases_structure.items():
            self._delete_database(database_name)
            