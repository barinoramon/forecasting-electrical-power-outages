from collections import namedtuple

class NamedTuple:
    def __init__(self, name:str, fields:list, values:list):
        self.name = tuple_name
        self.fields = tuple_fields
        self.values = values
        self.tuple = self._set_tuple()
        
    def _format_values_dictionary(self):
        return {key: value for key, value in zip(self.fields, self.values)}

    def _set_tuple(self):
        tuple_format = namedtuple(self.tuple_name, self.tuple_fields)
        tuple_values = self._format_values_dictionary()
        return tuple_format(**tuple_values)

    def get(self):
        return self.tuple