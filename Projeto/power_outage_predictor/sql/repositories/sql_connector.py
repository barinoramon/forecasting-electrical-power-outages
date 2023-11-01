import mysql.connector
from configs.configs import Config
from general.singleton.singleton import SingletonMeta


class SqlConnector(metaclass=SingletonMeta):
    def __init__(self):
        config = Config()
        self.connector = mysql.connector.connect(
            host=config.sql_params['HOST'],
            user=config.sql_params['USERNAME'],
            passwd=config.sql_params['PASSWORD'],
        )
        self.cursor = self.connector.cursor()
    
    def execute(self, query:str, params=None):
        self.cursor.execute(query, params)
        self.connector.commit()
        
    def fetch(self, query, params=None):
        self.cursor.execute(query, params)
        result = self.cursor.fetchall()
        return result
    
    def execute_many(self, query:str, params=None):
        self.cursor.executemany(query, params)
        self.connector.commit()
            
    def close(self):
        self.cursor.close()
        self.connector.close()
        