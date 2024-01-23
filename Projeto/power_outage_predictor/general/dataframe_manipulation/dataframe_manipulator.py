import pandas as pd

class DataframeManipulator:
    def __init__(self, dataframe:pd.DataFrame=None):
        self.dataframe = dataframe
    def set_dataframe(self, dataframe:pd.DataFrame):
        self.dataframe = dataframe
    def get_dataframe(self):
        return self.dataframe
    def add_columns(self, columns:list, values:list):
        self.dataframe = self.dataframe.assign(**dict(zip(columns, values)))
        return self.dataframe
    def rename_columns(self, columns:dict):
        self.dataframe = self.dataframe.rename(columns, axis=1)
        return self.dataframe
    def remove_columns(self, columns:list):
        self.dataframe = self.dataframe.drop(columns, axis=1)
        return self.dataframe
    def filter_rows(self, column, values:list):
        self.dataframe = self.dataframe[self.dataframe[column].isin(values)]
        return self.dataframe
    def join_dataframe(self, dataframe):
        if self.dataframe is None:
            self.set_dataframe(dataframe)
        else:
            self.dataframe = self.dataframe.join(dataframe)
        return self.dataframe