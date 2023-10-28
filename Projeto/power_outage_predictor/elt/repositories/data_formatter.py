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
        for column, tipo in self.columns_schema.items():
            if column in dataframe.columns:
                dataframe[column] = dataframe[column].astype(tipo)
        return dataframe
    def get(self):
        return self.data.get_dataframe()
    