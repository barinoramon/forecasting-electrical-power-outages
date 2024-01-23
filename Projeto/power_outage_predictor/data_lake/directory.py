import os
import pandas as pd
from data_lake.file import File

class Directory:
    def __init__(self, path:str):
        self.path = path

    def list_files(self):
        return [os.path.join(self.path,f) for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
    
    def list_directories(self):
        return [f for f in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, f))]
    
    def file_exists(self, file_path):
        return os.path.exists(os.path.join(self.path, file_path))
    
    def get_file(self, file_path):
        return File(os.path.join(self.path, file_path))
    
    def delete_file(self, file_path):
        os.remove(os.join(self.path, file_path))

    def save_excel(self, dataframe:pd.DataFrame, path):
        dataframe.to_excel(os.join(self.path, path), index=False, header=True)
        
    def save_csv(self, dataframe:pd.DataFrame, path):
        dataframe.to_csv(os.join(self.path, path), index=False, header=True)
    


        
    
        
        
    