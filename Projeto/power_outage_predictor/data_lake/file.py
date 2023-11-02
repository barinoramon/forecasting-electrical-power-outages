import os
from collections import namedtuple
import pandas as pd

class File:
    def __init__(self, path):
        self.path = path
        self.name, self.format = os.path.splitext(self.path)
        
    def read_csv(self, delimiter=',', encoding='utf-8', encoding_errors='strict'):
        return pd.read_csv(self.path, delimiter=delimiter, encoding=encoding, encoding_errors=encoding_errors)
    
    def read_excel(self):
        return pd.read_excel(self.path)
    def read(self):
        if self.format=='.csv':
            return pd.read_csv(self.path, delimiter=';')
        elif self.format=='.xlsx':
            return pd.read_excel(self.path)