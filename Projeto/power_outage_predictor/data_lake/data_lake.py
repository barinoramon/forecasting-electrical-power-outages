import sys
import os
from data_lake.file import File
from collections import namedtuple
from data_lake.directory import Directory


class DataLake:
    #data_lake_path = r"D:\Documentos\UFF\TCC\Projeto\Data Lake" PROD
    data_lake_path = r"D:\Documentos\UFF\TCC\Projeto\Test Data Lake"
    
    def __init__(self):
        pass
    
    def _path_exists(self, path):
        return os.path.exists(self._join_with_data_lake_path(path))
    def _path_is_directory(self, path):
        return os.path.isdir(self._join_with_data_lake_path(path))
    def _path_is_file(self, path):
        return os.path.isfile(self._join_with_data_lake_path(path))
    def _join_with_data_lake_path(self, path):
        return os.path.join(self.data_lake_path, path)
    
    def directory_exists(self, directory_path):
        directory_path = self._join_with_data_lake_path(directory_path)
        return self._path_exists(directory_path) and self._path_is_directory(directory_path)
    def file_exists(self, file_path):
        file_path = self._join_with_data_lake_path(file_path)
        return self._path_exists(file_path) and self._path_is_file(file_path)
    
    def create_directory(self, directory_path):
        directory_path = self._join_with_data_lake_path(directory_path)
        if not self.directory_exists(directory_path):
            os.mkdir(os.path.join(self.data_lake_path, directory_path))
        else: print("Directory already exists: {}".format(directory_path)) 
    def delete_directory(self, directory_path):
        directory_path = self._join_with_data_lake_path(directory_path)
        if self.directory_exists(directory_path):
            os.rmdir(os.path.join(self.data_lake_path, directory_path))
        else: print("Directory not found: {}".format(directory_path))
    def get_directory(self, directory_path):
        directory_path = self._join_with_data_lake_path(directory_path)
        assert self.directory_exists(directory_path), "Directory not found: {}".format(directory_path)
        return Directory(directory_path)
    def get_file(self, file_path):
        file_path = self._join_with_data_lake_path(file_path)
        assert self.file_exists(file_path), "File not found: {}".format(file_path)
        return File(file_path)
    
    
