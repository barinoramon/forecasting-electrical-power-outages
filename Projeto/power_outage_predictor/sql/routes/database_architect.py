from sql.repositories.database_constructor import DatabaseConstructor

class DatabaseArchitect:
    def __init__(self):
        self.database_constructor = DatabaseConstructor()
        self.database_project = {
            "ingested": [
                {"name": "power_outages", 
                 "columns": [("conjunto_eletrico", "varchar(255)"), ("tipo_interrupcao", "varchar(255)"), ("data_inicio", "varchar(255)"), 
                                   ("data_fim", "varchar(255)"), ("causa", "varchar(255)"), ("n_consumidor_conjunto", "varchar(255)")]}
                ]
        }
    def define_project(self):
        self.database_constructor.set_architecture(self.database_project)
    def build(self):
        self.database_constructor.build()