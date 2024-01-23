import mysql.connector
from configs.configs import Config
from general.singleton.singleton import SingletonMeta


class SqlConnector(metaclass=SingletonMeta):
    def __init__(self):
        self.config = Config()
        self.username = self.config.sql_params['USERNAME']
        self.password = self.config.sql_params['PASSWORD']
        self.host = self.config.sql_params['HOST']
        self.connector = mysql.connector.connect(
            host=self.host,
            user=self.username,
            passwd=self.password,
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
        