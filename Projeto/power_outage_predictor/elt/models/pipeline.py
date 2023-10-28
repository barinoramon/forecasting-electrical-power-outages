from abc import ABC, abstractmethod

class Pipeline(ABC):
    def __init__(self):
        self.data_source = None
         
    @abstractmethod
    def script(self):
        pass