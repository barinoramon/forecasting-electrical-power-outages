import mysql.connector


class SqlConnector:
    def __init__(self):
        self.connector = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="youtube2000",
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
        