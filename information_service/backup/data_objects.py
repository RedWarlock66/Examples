#module for describing objects which manages information service data
import copy

#core object to work with data
class DataRelation:
    def __init__(self, attributes:list): #attributes: list of metadata.MetadataAttribute
        self.__attributes = attributes
        self.__records = [] #list of DataRecord

    def __iter__(self):
        return iter(self.__records)

    def __repr__(self):
        return "\n".join([str(record) for record in self.__records])

    @property
    def attributes(self):
        return copy.deepcopy(self.__attributes)

    def add_record(self, **values):
        record = DataRecord(self, **values)
        self.__records.append(record)
        return record

    def drop_record(self, index):
        self.__records.pop(index)

    def get_record(self, index):
        return self.__records[index]

#single record of DataRelation
class DataRecord:
    def __init__(self, owner:DataRelation, **values):
        self.__dict__["_owner"] = owner
        self.__dict__["_attributes"] = {attribute.name: attribute.value_type for attribute in self._owner.attributes}
        for attribute in self._owner.attributes:
            if attribute.name in values:
                self.__setattr__(attribute.name, values[attribute.name])
            else:
                self.__setattr__(attribute.name, attribute.default)

    def __setattr__(self, key, value):
        if  key in self._attributes:
            self.__check_value_type(key, value)
            self.__dict__[key] = value

    def __repr__(self):
        return str(self.values)

    @property
    def values(self) -> dict:
        return {attribute:self.__dict__[attribute] for attribute in self._attributes.keys()}

    def __check_value_type(self, key, value):
        if not isinstance(value, self._attributes[key]):
            raise ValueError(f"Value {value} for attribute {key} is not type {self._attributes[key]}!")