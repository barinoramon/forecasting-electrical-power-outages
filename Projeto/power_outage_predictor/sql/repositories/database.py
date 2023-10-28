import mysql.connector
from sql.repositories.sql_connector import SqlConnector
from sql.repositories.stored_procedures_manager import StoredProcedureManager


class Database(SqlConnector):
    def __init__(self, name:str):
        super().__init__()
        self.name:str = name
        self.exists:bool = self._database_exists(self.name)
        self.tables = self._identify_tables()
        self.stored_procedures = StoredProcedureManager(self.cursor, self.name)
    
    def _database_exists(self, name:str) -> bool:
        return name in self.get_existing_databases_list()
        
    def _identify_tables(self) -> list:
        if self.exists:
            self.cursor.execute(f"SHOW TABLES FROM {self.name}")
            return [str(table[0]) for table in self.cursor.fetchall()]
        else:
            return []
    
    def get_existing_databases_list(self):
        database_list = self.fetch("SHOW DATABASES")
        return [str(database[0]) for database in database_list]
    
    def get_existing_tables_list(self):
        return self._identify_tables()

    def create(self):
        query = f"CREATE DATABASE IF NOT EXISTS {self.name}"
        self.execute(query)
        self.exists = True
    
    def delete(self):
        if self.exists:
            query = f"DROP DATABASE {self.name}"
            self.execute(query)
            self.exists = False
            
    def use(self):
        query = f"USE {self.name}"
        self.execute(query)
    
    def get_table(self, table_name:str):
        return Table(database=self, name=table_name)
    
        
class Table:
    import pandas as pd
    def __init__(self, database:Database, name:str):
        self.database = database
        self.cursor = database.cursor
        self.name = name
        self.exists = self._table_exists()

    def _table_exists(self):
        query = f"SHOW TABLES FROM {self.database.name} LIKE '{self.name}'"
        result = self.database.fetch(query)
        return len(result) > 0
    
    def create(self, columns):
        columns_str = ", ".join([f"{col_name} {col_type}" for col_name, col_type in columns])
        query = f"CREATE TABLE IF NOT EXISTS {self.name} ({columns_str})"
        self.database.execute(query)
        self.exists = self._table_exists()
    
    def delete(self):
        query = f"DROP TABLE IF EXISTS {self.name}"
        self.database.execute(query)
        self.exists = self._table_exists()
    
    def delete_values(self, where_clause, params=None):
        query = f"DELETE FROM {self.name} WHERE {where_clause}"
        self.database.execute(query, params)
        self.exists = self._table_exists()

    def select(self):
        query = f"SELECT * FROM {self.name}"
        result = self.database.fetch(query)
        return result

    def insert(self, column_name, values):
        placeholders = ", ".join(["%s" for _ in values])
        query = f"INSERT INTO {self.name} VALUES ({placeholders})"
        self.database.execute(query, values)
        
    def insert_dataframe(self, dataframe:pd.DataFrame):
        '''TODO: tentar achar uma forma de inserir o dataframe como um todo
        insert_query = f"INSERT INTO {self.name} ({', '.join(dataframe.columns)}) VALUES ({', '.join(['%s'] * len(dataframe.columns))})"
        values = [tuple(row) for row in dataframe.values]
        
        for row in dataframe.values:
            insert_query = f"INSERT INTO {self.name} ({', '.join(dataframe.columns)}) VALUES ({', '.join(['%s'] * len(dataframe.columns))})"
            values = [tuple(row) for row in dataframe.values]
        '''
        for index, row in dataframe.iterrows():
            insert_query = f"INSERT INTO {self.name} ({', '.join(dataframe.columns)}) VALUES ({', '.join(['%s'] * len(dataframe.columns))})"
            self.database.execute(insert_query, tuple(row))

    def update(self, set_clause, where_clause, params=None):
        query = f"UPDATE {self.name} SET {set_clause} WHERE {where_clause}"
        self.database.execute(query, params)
        
    def get_table_length(self):
        query = f"SELECT COUNT(*) FROM {self.name}"
        result = self.database.fetch(query)
        return result[0][0]
    
    def get_table_dataframe(self):
        query = f"SELECT * FROM {self.name}"
        result = self.database.fetch(query)
        return result
    
    def get_table_columns(self):
        query = f"DESCRIBE {self.name}"
        result = self.database.fetch(query)
        return result
    
    def display(self):
        table_values = self.get_table_dataframe()
        print(table_values)
    