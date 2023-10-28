import os
from collections import namedtuple
import pandas as pd

class File:
    def __init__(self, path):
        self.path = path
        self.name, self.format = os.path.splitext(self.path)
        
    def read(self):
        if self.format=='.csv':
            return pd.read_csv(self.path)
        elif self.format=='.xlsx':
            return pd.read_excel(self.path)