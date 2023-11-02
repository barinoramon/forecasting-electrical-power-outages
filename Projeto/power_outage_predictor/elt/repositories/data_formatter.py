import pandas as pd
from general.dataframe_manipulation.dataframe_manipulator import DataframeManipulator

class DataFormatter:
    def __init__(self, data:pd.DataFrame, columns_to_be_renamed:dict, columns_to_be_removed:list, columns_schema:dict):
        self.data = DataframeManipulator(data)
        self.columns_to_be_renamed = columns_to_be_renamed
        self.columns_to_be_removed = columns_to_be_removed
        self.columns_schema = columns_schema
    def rename_columns(self):
        if len(self.columns_to_be_renamed) > 0:
            return self.data.rename_columns(self.columns_to_be_renamed)
    def remove_columns(self):
        if len(self.columns_to_be_removed) > 0:
            return self.data.remove_columns(self.columns_to_be_removed)
    def apply_schema(self):
        dataframe = self.data.get_dataframe()
        for column, column_type in self.columns_schema.items():
            if column in dataframe.columns:
                if column_type == float:
                    dataframe[column] = self._convert_column_to_float(dataframe, column)
                elif column_type == int:
                    dataframe[column] = self._convert_column_to_int(dataframe, column)
                elif column_type in [str]:
                    dataframe[column] = self._convert_column_to_string(dataframe, column)
        return dataframe
    
    def _convert_column_to_float(self, dataframe, column):
        dataframe = self.data.get_dataframe()
        dataframe[column] = dataframe[column].astype(str).replace(',', '.')
        dataframe[column] = dataframe[column].fillna(0).astype(float)
        return dataframe[column]
    
    def _convert_column_to_int(self, dataframe, column):
        dataframe[column] = dataframe[column].astype(str).replace(',', '.').astype(float).round().fillna(0).astype(int)
        return dataframe[column]
            
    def _convert_column_to_string(self, dataframe, column):
        dataframe[column] = dataframe[column].fillna("").astype(str)
        return dataframe[column]


    def get(self):
        return self.data.get_dataframe()
    