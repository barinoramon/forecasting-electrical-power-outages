#import mysql.cursor


class StoredProcedureManager:

    def __init__(self, cursor, database_name):
        self.cursor = cursor
        self.database_name = database_name


    def create_procedure(self, procedure_name, procedure_sql):
        query = f"CREATE PROCEDURE {procedure_name} {procedure_sql}"
        self.cursor.execute(query)


    def list_procedures(self):
        query = f"SHOW PROCEDURE STATUS WHERE Db = '{self.database_name}'"
        result = self.cursor.fetch(query)
        return [row[1] for row in result]


    def execute_stored_procedure(self, procedure_name, params=None):
        if params is None:
            query = f"CALL {procedure_name}()"
        else:
            placeholders = ", ".join(["%s" for _ in params])
            query = f"CALL {procedure_name}({placeholders})"
        self.cursor.execute(query, params)


    def drop_procedure(self, procedure_name):
        query = f"DROP PROCEDURE IF EXISTS {procedure_name}"
        self.cursor.execute(query)