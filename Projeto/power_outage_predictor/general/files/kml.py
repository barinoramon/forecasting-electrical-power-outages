import os
import zipfile
import pandas as pd
import geopandas as gpd

class KMZFile:
    def __init__(self, path):
        self.path = path
        self.name, self.format = os.path.splitext(self.path)

    def unzip(self, extracted_path='kml_files'):
        with zipfile.ZipFile(self.path, 'r') as zip_ref:
            kml_file = kmz.extract(kmz.namelist()[1], path='kml_files')
            

class KMLFile:
    def __init__(self, path):
        self.path = path
        self.name, self.format = os.path.splitext(self.path)
        self._set_supported_driver()
    
    def _set_supported_driver(self):
        gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    
    def to_dataframe(self):
        geo_dataframe = gpd.read_file(self.path, driver='KML')
        pandas_dataframe = pd.DataFrame(geo_dataframe)
        return pandas_dataframe
        
    