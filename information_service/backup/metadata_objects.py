#module for describing metadata objects

import re, datetime, uuid, copy
from abc import ABC, abstractmethod
from json import JSONEncoder

#service methods
def _check_name(value, **kwargs):
    if not isinstance(value, str):
        raise ValueError(f"Name {value} is not string!")
    if not value:
        raise ValueError("Name cannot be empty!")
    if not value[0].isalpha():
        raise ValueError(f"First symbol of {value} is not letter!")
    pattern = r"^[a-zA-Z0-9_]+$"
    #примечание - на \n регулярки почему-то не реагируют. потом разобраться, почему
    if not re.compile(pattern).search(value) or value.find("\n") > -1:
        raise ValueError(f"One of the symbols of {value} doesn't match {pattern}")

class RelationLink:
    def __init__(self, relation_id:str):
        self.__relation_id = relation_id

    def __repr__(self):
        return JSONEncoder.encode({"relation_id":self.__relation_id})

class _TypeManager(ABC):

    __allowed_types = {str:"", bool:False, int:0, float:0.0, datetime.datetime:datetime.datetime(1, 1, 1),
                       RelationLink:RelationLink("")}

    @classmethod
    def is_allowed(cls, value_type):
        return value_type in cls.__allowed_types

    @classmethod
    def default(cls, value_type):
        if cls.is_allowed(value_type):
            return cls.__allowed_types[value_type]
        else:
            return None

    @classmethod
    def allowed_types(cls):
        return list(cls.__allowed_types)

class _IDGenerator(ABC):
    @staticmethod
    def generate():
        return str(uuid.uuid4())

class _RelationIndex:
    def __init__(self, attributes:list, clustered:bool = False):
        self.__attributes = attributes
        self.__clustered = clustered

    @property
    def attributes(self):
        return self.__attributes

    @property
    def clustered(self):
        return self.__clustered

class MetadataAttribute:

    #magic methods

    def __init__(self, owner, name:str = "", store_name:str = "", value_type = str, not_null = False, default = ""):
        #ПРИМЕЧАНИЕ - КОСТЫЛЬ. ПОТОМ ОТКОПАТЬ СПОСОБ, КАК ПОЛУЧИТЬ ВСЕ АРГУМЕНТЫ ФУНКЦИИ С ИХ ЗНАЧЕНИЯМИ
        #ПРИМЕЧАНИЕ - ПРИ ДОБАВЛЕНИИ НОВОГО СВОЙСТВА АТРИБУТА - ВКЛЮЧИТЬ ЕГО В СОЗДАВАЕМОЕ ОПИСАНИЕ МЕТАДАННЫХ В metadata.Metadata
        self.__initial_values = {"name":name, "value_type":value_type, "not_null":not_null, "default":default}
        self.__changes = []
        self.__properties = {}
        self.__owner = owner
        self.__store_name = store_name
        self.__check_functions = {"name":_check_name, "value_type":self.__check_value_type, "not_null":self.__check_not_null,
                                  "default":self.__check_default}
        self.__set_property("name", name, False)
        self.__set_property("value_type", value_type, False)
        self.__set_property("not_null", not_null, False)
        if default:
            self.__set_property("default", default, False)
        else:
            self.__set_property("default", _TypeManager.default(self.value_type), False)

    def __repr__(self):
        return f"MetadataAttribute:{self.properties_values()}"

    #properies
    @property
    def store_name(self):
        return self.__store_name

    @property
    def name(self):
        return self.__get_property("name")

    @name.setter
    def name(self, value):
        self.__set_property("name", value)

    @property
    def value_type(self):
        return self.__get_property("value_type")

    @value_type.setter
    def value_type(self, value):
        self.__set_property("value_type", value)
        self.__set_property("default", _TypeManager.default(value))

    @property
    def not_null(self):
        return self.__get_property("not_null")

    @not_null.setter
    def not_null(self, value):
        self.__set_property("not_null", value)

    @property
    def default(self):
        return self.__get_property("default")

    @default.setter
    def default(self, value):
        self.__set_property("default", value)

    @property
    def changed(self):
        if self.__changes:
            return True
        else:
            return False

    #api

    def properties_values(self, changed_only:bool = False):
        if changed_only:
            return {key:copy.deepcopy(value) for key, value in self.__properties.items() if key in self.__changes}
        else:
            return copy.deepcopy(self.__properties)

    #service

    def __get_property(self, key:str):
        return copy.deepcopy(self.__properties[key])

    def __set_property(self, key:str, value, changed:bool = True):
        self.__check_property(key, value)
        self.__properties[key] = value
        if changed and not key in self.__changes and not self.__initial_values[key] == value:
            self.__changes.append(key)
        if self.__initial_values[key] == value and key in self.__changes:
            self.__changes.remove(key)

    def __check_property(self, key:str, value):
        self.__owner._check_attribute_property(key, value)
        if key in self.__check_functions:
            self.__check_functions[key](value)

    def __check_value_type(self, value):
        if not _TypeManager.is_allowed(value):
            raise TypeError(f"Type {value} is not in allowed types {_TypeManager.allowed_types()}")

    def __check_not_null(self, value):
        if not isinstance(value, bool):
            raise ValueError(f"Not null should be bool, not {value}")

    def __check_default(self, value):
        if not isinstance(value, self.value_type):
            raise ValueError(f"Default value {value} is not {self.value_type}")

class MetadataRelation(ABC):
    #ПРИМЕЧАНИЕ - ЗДЕСЬ И ДАЛЕЕ СЧИТАЕМ, ЧТО СЕАНС, ИЗМЕНЯЮЩИЙ СТРУКТУРУ МЕТАДАННЫХ - ЕДИНСТВЕННЫЙ, А ИЗМЕНЕНИЯ СТРУКТУРЫ
    #ЗАПРЕЩЕНЫ, ПОКА СУЩЕСТВУЮТ ДРУГИЕ СЕАНСЫ. ПОТОМ ПРИСОБАЧИТЬ МЕХАНИЗМ, КОТОРЫЙ ОБЕСПЕЧИЛ БЫ И ПЕРВОЕ, И ВТОРОЕ

    #ПРИМЕЧАНИЕ - МЕХАНИКУ ДОПОЛНИТЕЛЬНЫХ ИНДЕКСОВ ПРИКРУТИТЬ КАК-НИБУДЬ ПОТОМ

    def __init__(self, owner, id:str, md_type = str, store_name:str = "", name:str = "", attributes:dict = {}):
        self._owner = owner
        self._id = id
        self._md_type = md_type
        self._store_name = store_name
        self._name = self._set_name(name)
        self._initial_name = self._name
        self._attributes = attributes
        self._added_attributes = []
        self._dropped_attributes = []
        self._standard_attributes = {"id":MetadataAttribute(self, "id", str, True),
                                     "parent_id":MetadataAttribute(self, "parent_id", str, True),
                                     "name":MetadataAttribute(self, "name", str, True),
                                     "is_folder":MetadataAttribute(self, "is_folder", bool, True),
                                     "deletion_mark":MetadataAttribute(self, "deletion_mark", bool, True)
                                    }

    def __repr__(self):
       standard_attributes = "\n".join([f"{value}" for value in self._standard_attributes.values()])
       attributes = "\n".join([f"{value}" for value in self._attributes.values()])
       return f"MetadataObjectRelation {self.__name}\nstandard attributes:\n{standard_attributes}\n" \
              f"attributes:\n{attributes}"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._owner._check_name(self._md_type, value)
        self._name = self._set_name(value)

    @property
    def attributes(self):
        return copy.deepcopy(self._attributes)

    @property
    def standard_attributes(self):
        return copy.deepcopy(self._standard_attributes)

    @property
    def added_attributes(self):
        return {key:copy.deepcopy(value) for key, value in self._attributes.items() if key in self._added_attributes}

    @property
    def dropped_attributes(self):
        return copy.deepcopy(self._dropped_attributes)

    @property
    def altered_atrributes(self):
        return {key: copy.deepcopy(value) for key, value in self._attributes.items() if value.changed
                and not key in self._added_attributes and not key in self._dropped_attributes}

    @property
    def changed(self):
        for attribute in self._attributes:
            if attribute.changed:
                return True
        return (self._added_attributes or self._dropped_attributes)

    @property
    def indexes(self):
        return self._standard_indexes

    def add_attribute(self, name:str = "", value_type:type = str, not_null:bool = False, default = "") -> MetadataAttribute:
        _attribute = MetadataAttribute(owner=self, name=name, value_type=value_type, not_null=not_null, default=default)
        if not _attribute.name:
            _attribute.name = f"Attribute{len(self._attributes)+1}"
        else:
            self._check_attribute_name(_attribute.name)
        attribute_id = _IDGenerator.generate()
        self._attributes[attribute_id] = _attribute
        self._added_attributes.append(attribute_id)
        return _attribute

    def drop_attribute(self, attribute_id):
        if not attribute_id in self.__attributes:
            raise ValueError(f"There is no attribute {attribute_id}")
        self.__attributes.pop(attribute_id)
        if attribute_id in self.__added_attributes:
            self.__added_attributes.remove(attribute_id)
        else:
            self.__dropped_attributes.append(attribute_id)

    def get_attribute(self, attribute_id) -> MetadataAttribute:
        if not attribute_id in self.__attributes:
            raise ValueError(f"There is no attribute {attribute_id}")
        return self.__attributes[attribute_id]

    #service
    def _check_attribute_property(self, key:str, value):
        if key == "name":
            for attribute in self.__attributes.values():
                if attribute.name == value:
                    raise ValueError(f"There is an attribute named {value}! Choose another name")

    def __set_name(self, name):
        _check_name(name)
        return name

    def __check_attribute_name(self, name):
        if name in self._standard_attributes:
            raise ValueError(f"There is a standard attribute named {name}! Choose another name")
        for attribute in self.__attributes.values():
            if attribute.name == name:
                raise ValueError(f"There is an attribute named {name}! Choose another name")

class MetadataObjectRelation(MetadataRelation):
    #ПРИМЕЧАНИЕ - ЗДЕСЬ И ДАЛЕЕ СЧИТАЕМ, ЧТО СЕАНС, ИЗМЕНЯЮЩИЙ СТРУКТУРУ МЕТАДАННЫХ - ЕДИНСТВЕННЫЙ, А ИЗМЕНЕНИЯ СТРУКТУРЫ
    #ЗАПРЕЩЕНЫ, ПОКА СУЩЕСТВУЮТ ДРУГИЕ СЕАНСЫ. ПОТОМ ПРИСОБАЧИТЬ МЕХАНИЗМ, КОТОРЫЙ ОБЕСПЕЧИЛ БЫ И ПЕРВОЕ, И ВТОРОЕ

    _primary_key = "id"
    _standard_indexes = (_RelationIndex(["id"], True), _RelationIndex(["parent_id", "id"]), _RelationIndex(["name", "id"]))
    #ПРИМЕЧАНИЕ - МЕХАНИКУ ДОПОЛНИТЕЛЬНЫХ ИНДЕКСОВ ПРИКРУТИТЬ КАК-НИБУДЬ ПОТОМ

    def __init__(self, owner, id:str, md_type = str, store_name:str = "", name:str = "", attributes:dict = {}):
        self.__owner = owner
        self.__id = id
        self.__md_type = md_type
        self.__store_name = store_name
        self.__name = self.__set_name(name)
        self.__initial_name = self.__name
        self.__attributes = attributes
        self.__added_attributes = []
        self.__dropped_attributes = []
        self.__standard_attributes = {"id":MetadataAttribute(self, "id", str, True),
                                     "parent_id":MetadataAttribute(self, "parent_id", str, True),
                                     "name":MetadataAttribute(self, "name", str, True),
                                     "is_folder":MetadataAttribute(self, "is_folder", bool, True),
                                     "deletion_mark":MetadataAttribute(self, "deletion_mark", bool, True)
                                    }

    def __repr__(self):
       standard_attributes = "\n".join([f"{value}" for value in self._standard_attributes.values()])
       attributes = "\n".join([f"{value}" for value in self.__attributes.values()])
       return f"MetadataObjectRelation {self.__name}\nstandard attributes:\n{standard_attributes}\n" \
              f"attributes:\n{attributes}"

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, value):
        self.__owner._check_name(self.__md_type, value)
        self.__name = self.__set_name(value)

    @property
    def attributes(self):
        return copy.deepcopy(self.__attributes)

    @property
    def standard_attributes(self):
        return copy.deepcopy(self.__standard_attributes)

    @property
    def added_attributes(self):
        return {key:copy.deepcopy(value) for key, value in self.__attributes.items() if key in self.__added_attributes}

    @property
    def dropped_attributes(self):
        return copy.deepcopy(self.__dropped_attributes)

    @property
    def altered_atrributes(self):
        return {key: copy.deepcopy(value) for key, value in self.__attributes.items() if value.changed
                and not key in self.__added_attributes and not key in self.__dropped_attributes}

    @property
    def changed(self):
        for attribute in self.__attributes:
            if attribute.changed:
                return True
        return (self.__added_attributes or self.__dropped_attributes)

    @property
    def indexes(self):
        return self._standard_indexes

    def add_attribute(self, name:str = "", value_type:type = str, not_null:bool = False, default = "") -> MetadataAttribute:
        _attribute = MetadataAttribute(owner=self, name=name, value_type=value_type, not_null=not_null, default=default)
        if not _attribute.name:
            _attribute.name = f"Attribute{len(self.__attributes)+1}"
        else:
            self.__check_attribute_name(_attribute.name)
        attribute_id = _IDGenerator.generate()
        self.__attributes[attribute_id] = _attribute
        self.__added_attributes.append(attribute_id)
        return _attribute

    def drop_attribute(self, attribute_id):
        if not attribute_id in self.__attributes:
            raise ValueError(f"There is no attribute {attribute_id}")
        self.__attributes.pop(attribute_id)
        if attribute_id in self.__added_attributes:
            self.__added_attributes.remove(attribute_id)
        else:
            self.__dropped_attributes.append(attribute_id)

    def get_attribute(self, attribute_id) -> MetadataAttribute:
        if not attribute_id in self.__attributes:
            raise ValueError(f"There is no attribute {attribute_id}")
        return self.__attributes[attribute_id]

    #service
    def _check_attribute_property(self, key:str, value):
        if key == "name":
            for attribute in self.__attributes.values():
                if attribute.name == value:
                    raise ValueError(f"There is an attribute named {value}! Choose another name")

    def __set_name(self, name):
        _check_name(name)
        return name

    def __check_attribute_name(self, name):
        if name in self._standard_attributes:
            raise ValueError(f"There is a standard attribute named {name}! Choose another name")
        for attribute in self.__attributes.values():
            if attribute.name == name:
                raise ValueError(f"There is an attribute named {name}! Choose another name")

#service

class _MetadataDescription:
    def __init__(self, name: str, primary_key:str):
        self.__name = name
        self.__attributes = []
        self.__indexes = []
        self.__primary_key = primary_key

    @property
    def name(self):
        return self.__name

    @property
    def attributes(self):
        return self.__attributes

    @property
    def indexes(self):
        return self.__indexes

    @property
    def primary_key(self):
        return self.__primary_key

    def add_attribute(self, **kwargs):
        attribute = MetadataAttribute(owner = self, **kwargs)
        self.__attributes.append(attribute)

    def add_index(self, **kwargs):
        index = _RelationIndex(**kwargs)
        self.__indexes.append(index)

    def _check_attribute_property(self, key, value):
        pass




