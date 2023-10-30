import mysql.connector
from configs.configs import db_host, db_username, db_password


class SqlConnector:
    def __init__(self):
        self.connector = mysql.connector.connect(
            host=db_host,
            user=db_username,
            passwd=db_password,
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
        