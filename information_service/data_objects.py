from information_service.metadata import Metadata
from information_service.md_objects import MDObject, MDAttribute, MDObjectsTypes
from information_service.data_managers import data_managers_factory
from information_service.basics import IDGenerator, TypeManager, ObjectLink
from copy import deepcopy
from abc import ABC, abstractmethod

class ReferenceManager:
    def __init__(self, infobase:str, name:str):
        self.__infobase = infobase
        self.__name = name

    def create_object(self):
        return ReferenceObject(self.__infobase, self.__name)

    def get_object(self, _id:str):
        _object = ReferenceObject(self.__infobase, self.__name, _id=_id)
        _object.read()
        return _object

    def find_object(self, _filter:dict):
        data_relation = _DataRelation(self.__infobase, MDObjectsTypes.reference, self.__name)
        data_relation.read_data(_filter)
        if data_relation.is_empty:
            return None
        else:
            return ReferenceObject(self.__infobase, self.__name, **data_relation[0].values())

    def get_list(self, read:bool = True, _filter:dict = None):
        return ReferenceList(self.__infobase, self.__name, read, _filter)

class DataManager:

    _managers = {MDObjectsTypes.reference:ReferenceManager}

    def __init__(self, infobase:str):
        self.__infobase = infobase

    def reference_manager(self, name:str):
        return self._get_manager(MDObjectsTypes.reference, name)

    def _get_manager(self, md_type:str, name:str):
        #ПРИМЕЧАНИЕ - СЮДА МОЖНО ПРОВЕРКУ НА НАЛИЧИЕ ТАКОГО ОБЪЕКТА
        return self._managers[md_type](infobase=self.__infobase, name=name)


class LinkObject(ABC):
    @abstractmethod
    def __init__(self, infobase:str, name:str, md_type:str, **values):
       #ПРИМЕЧАНИЕ - ТРЕТИЙ АРГУМЕНТ НЕ ОЧЕНЬ ПО СОЛИДУ (У ДОЧЕРНИХ ОБЪЕКТОВ НЕТ)
       #НО КЛАСС АБСТРАКТНЫЙ, ТАК ЧТО НЕ КРИТИЧНО
       self.__dict__["_metadata_id"] = Metadata(infobase).object_settings(md_type=md_type, name=name)["_id"]
       self.__dict__["_metadata_name"] = name

    def md_link(self):
        return self._metadata_id

    def md_name(self):
        return self._metadata_name

class ReferenceObject(LinkObject):
    def __init__(self, infobase:str, name:str, **values):
        super().__init__(infobase, name, MDObjectsTypes.reference, **values)
        self.__data_relation = _DataRelation(infobase, MDObjectsTypes.reference, name)
        self.__data_relation.add_record(**values)
        if not self._id:
            self.__data_relation[0]._id = IDGenerator.generate()

    def __getattr__(self, item):
        if item in self.__data_relation.attributes:
            return getattr(self.__data_relation[0], item)
        else:
            return self.__dict__[item]

    def __setattr__(self, key, value):
        if key == "_ReferenceObject__data_relation":
            self.__dict__["_ReferenceObject__data_relation"] = value
        elif key == "_id": #_id cannot be setted manually
            pass
        elif key in self.__data_relation.attributes:
            setattr(self.__data_relation[0], key, value)
        else:
            self.__dict__[key] = value

    @property
    def values(self):
        return self.__data_relation[0].values()

    def encoded_values(self):
        return self.__data_relation[0].values(encoded=True)

    def read(self):
        _filter = {"_id":self._id}
        self.__data_relation.read_data(_filter)
        if self.__data_relation.is_empty:
            self.__data_relation.add_record(**_filter)

    def write(self):
        if not self._id:
            self.__data_relation[0]._id = IDGenerator.generate()
        _filter = {"_id": self._id}
        self.__data_relation.write_data(_filter)

    def delete(self):
        self._del_mark = True

    def restore(self):
        self._del_mark = False


class ReferenceList:
    def __init__(self, infobase:str, name:str, read:bool = False, _filter:dict = None):
        self.__infobase = infobase
        self.__name = name
        self.__elements = []
        if read:
            self.read(_filter)

    def __getitem__(self, item):
        return self.__elements[item]

    def __iter__(self):
        return iter(self.__elements)

    @property
    def infobase(self):
        return self.__infobase

    @property
    def name(self):
        return self.__name

    def read(self, _filter:dict = None):
        self.__elements.clear()
        data_relation = _DataRelation(self.__infobase, MDObjectsTypes.reference, self.__name)
        data_relation.read_data(_filter)
        for data_record in data_relation:
            self.__elements.append(_ReferenceListElement(self, data_record))

class _ReferenceListElement:
    def __init__(self, owner:ReferenceList, data_record):
        self.__owner = owner
        self.__data_record = data_record

    def __getattr__(self, item):
        return getattr(self.__data_record, item)

    @property
    def values(self):
        return self.__data_record.values()

    def get_object(self):
        return ReferenceObject(self.__owner.infobase, self.__owner.name, **self.values)

class _DataRelation:
    def __init__(self, infobase:str, md_type:str, name:str):
        if not md_type in Metadata._available_md_types:
            raise ValueError(f"metadata type {md_type} is not available!")
        self.__infobase = infobase
        self.__data_manager = data_managers_factory.create(infobase)
        self.__metadata = self.__data_manager.read_data(storage=Metadata._metadata_storages[MDObject],
                                                          filter={"md_type":md_type, "name":name})[0]
        self.__add_attributes(md_type)
        self.__records = []

    def __repr__(self):
        return "\n".join([str(record) for record in self.__records])

    def __getitem__(self, item):
        return self.__records[item]

    def __iter__(self):
        return iter(self.__records)

    @property
    def attributes(self) -> dict:
        return deepcopy(self.__attributes)

    @property
    def is_empty(self):
        return not self.__records

    def values(self, by_store_name:bool = False):
        return [record.values(by_store_name) for record in self.__records]

    def read_data(self, filter:dict = None):
        self.clear()
        _filter = self.__get_filter(filter)
        data_array = self.__data_manager.read_data(storage=self.__metadata["store_name"], filter=_filter, direct_storage_name=True)
        for data_string in data_array:
            data_record = {self.__attributes_store_names[key]:self.__decode_value(self.__attributes_store_names[key], value)
                           for key, value in data_string.items()}
            self.add_record(**data_record)

    def write_data(self, filter:dict = None):
        _filter = self.__get_filter(filter)
        self.__data_manager.write_data(storage=self.__metadata["store_name"], data=self.__records, filter=_filter, direct_storage_name=True)

    def add_record(self, **values):
        record = _DataRecord(owner=self, **values)
        self.__records.append(record)
        return record

    def drop_record(self, record):
        self.__records.pop(record)

    def clear(self):
        self.__records.clear()

    def __get_filter(self, filter:dict = None):
        if filter is None:
            return None
        _filter = {}
        for key, value in filter.items():
            if not key in self.__attributes:
                raise ValueError(f"There is no attribute named {key}")
            self.__attributes[key]._check_value(value)
            _filter[self.__store_name(key)] = value
        return _filter

    def __store_name(self, name):
        return self.__attributes[name].store_name

    def __add_attributes(self, md_type:str):
        self.__attributes = {}
        self.__attributes_store_names = {}
        self.__add_standard_attributes(md_type)
        self.__add_custom_attributes()

    def __add_standard_attributes(self, md_type:str):
        md_object = Metadata._available_md_types[md_type]
        if not md_object.standard_attributes is None:
            for attribute in md_object.standard_attributes:
                self.__attributes[attribute.store_name] = \
                    _DataAttribute(store_name=attribute.store_name, value_type=attribute.value_type,
                                   default=attribute.default)
                self.__attributes_store_names[attribute.store_name] = attribute.store_name

    def __add_custom_attributes(self):
        attributes_data = self.__data_manager.read_data(storage=Metadata._metadata_storages[MDAttribute],
                                                        filter={"object_id":self.__metadata["_id"]})
        for attribute_data in attributes_data:
            self.__attributes[attribute_data["name"]] = _DataAttribute(**self.__attribute_arguments(attribute_data))
            self.__attributes_store_names[attribute_data["store_name"]] = attribute_data["name"]

    def __attribute_arguments(self, attribute_data):
        return {_property:MDAttribute.decode_value(_property, value) for _property, value in attribute_data.items()}

    def __decode_value(self, attribute_name, value):
        if isinstance(self.__attributes[attribute_name].value_type, ObjectLink):
            if not value:
                return None
            else:
                _link = self.__attributes[attribute_name].value_type.decoded_link(value)
                object_settings = Metadata(self.__infobase).object_settings(object_id=_link["object_id"])
                _manager = DataManager(self.__infobase)._get_manager(object_settings["md_type"], object_settings["name"])
                _object = _manager.find_object({"_id":_link["id"]})
                _object.read()
                return _object
        else:
            return value

class _DataAttribute:
    def __init__(self, store_name:str, value_type:type, default, name:str = None, **kwargs):
        if name is None:
            self.__name = store_name
        else:
            self.__name = name
        self.__store_name = store_name
        self.__value_type = value_type
        self.__default = default

    def __repr__(self):
        return f"Attribute {self.__name}:store_name:{self.__store_name} value_type:{self.__value_type} default {self.__default}"

    @property
    def name(self):
        return self.__name

    @property
    def store_name(self):
        return self.__store_name

    @property
    def value_type(self):
        return self.__value_type

    @property
    def store_type(self):
        if isinstance(self.__value_type, ObjectLink):
            return str
        else:
            return self.__value_type

    @property
    def default(self):
        return self.__default

    def _check_value(self, value):
        if not TypeManager.istype(value, self.__value_type):
            raise TypeError(f"Value {value} is not {self.__value_type}")

class _DataRecord:
    def __init__(self, owner:_DataRelation, **values):
        self._owner = owner
        self.__add_attributes()
        self.fill(**values)

    def __repr__(self):
        return str(self.values())

    def __setattr__(self, key, value):
        if key == "_owner":
            self.__dict__[key] = value
        elif key in self._owner.attributes:
            _attribute = self._owner.attributes[key]
            if isinstance(_attribute.value_type, ObjectLink):
               self.__set_linked_value(key, value)
            else:
                _attribute._check_value(value)
                self.__dict__[key] = value
        else:
            self.__dict__[key] = value

    def values(self, by_store_names:bool = False, encoded:bool = False):
        return {self.__attribute_key(attr, by_store_names):self.__get_value(attr, encoded) for attr in self._owner.attributes.values()}

    def fill(self, **values):
        for key, value in values.items():
            setattr(self, key, value)

    def __attribute_key(self, attr, by_store_name:bool = False):
        if by_store_name:
            return attr.store_name
        else:
            return attr.name

    def __add_attributes(self):
        for attribute in self._owner.attributes.values():
            self.__dict__[attribute.name] = attribute.default

    def __set_linked_value(self, key, value):
        if not isinstance(value, LinkObject) and not value is None:
            raise TypeError(f"Value {value} isn't link object!")
        self.__dict__[key] = value

    def __get_value(self, _attribute:_DataAttribute, encoded:bool):
        if encoded:
            return self.__encoded_value(_attribute)
        else:
            return getattr(self, _attribute.name)

    def __encoded_value(self, _attribute:_DataAttribute):
        value = getattr(self, _attribute.name)
        if isinstance(_attribute.value_type, ObjectLink):
            if value is None:
                return ""
            else:
                return _attribute.value_type.encoded_link(value.md_link(), value._id)
        else:
            return value