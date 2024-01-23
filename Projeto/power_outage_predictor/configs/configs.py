import os
import json
from general.singleton.singleton import SingletonMeta

current_directory = os.path.dirname(os.path.abspath(__file__))

class Config(metaclass=SingletonMeta):
    def __init__(self):
        self.sql_params = {
            "USERNAME": os.environ.get("db_username"),
            "HOST": os.environ.get("db_host"),
            "PASSWORD": os.environ.get("db_password")
        }
        self.paths = self._set_paths()
        self.db_architectures = self._set_databases_architectures()
        self.assets = self._set_assets()
    
    def _set_paths(self):
        with open(os.path.join(current_directory, 'paths.json'), 'r') as json_paths:
            paths = json.load(json_paths)
        return paths

    def _set_databases_architectures(self):
        with open(os.path.join(current_directory, 'databases.json'), 'r') as json_databases:
            db_architectures = json.load(json_databases)['databases_architecture']
        return db_architectures
    
    def _set_assets(self):
        with open(os.path.join(current_directory, 'assets.json'), 'r') as json_assets:
            assets = json.load(json_assets)
        return assets

