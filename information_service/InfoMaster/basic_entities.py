#module for basic classes, which may be used in any other module

import copy, pathlib, stat
import uuid
from datetime import datetime

#service methods
def _relation_attributes(attributes:dict, **_attributes):
    if not attributes and len(_attributes) == 0:
        raise ValueError("No attributes have been given!")
    __attributes = {}
    if len(_attributes) > 0:
        __attributes.update(_attributes)
    if attributes:
        for i in attributes:
            if not isinstance(i, str):
                raise TypeError("attribute name is not string: " + str(i))
        __attributes.update(attributes)
    for _name, _type in __attributes.items():
        if _type != None:
            if not isinstance(_type, tuple) and not isinstance(_type, type):
                raise TypeError(f"Invalid type {_type} of attribute {_name}! Type must be type or tuple of values")
    return __attributes

#API

#Classes
#table
#attributes - list. key - attribute name -> str, value - attribute type -> type
class ResultDescription:
    def __init__(self, success:bool = False, result = None):
        self.success = success
        self.result  = result

    def __repr__(self):
        return f"success:{self.success}\nresult:{self.result}"

class TypeManager():

    #ПРИМЕЧАНИЕ - ПОТОМ НАЙТИ МЕТОД БОЛЕЕ ИЗЯЩНЫЙ МЕТОД ПРЕОБРАЗОВАНИЯ СТОРОКОВОГО ПРЕДСТАВЛЕНИЯ В ТИП И ТИПА В ДЕФОЛТНОЕ ЗНАЧЕНИЕ
    _types = {"str":str, "int":int, "bool":bool, "float":float, "datetime.datetime":datetime}
    _default_values = {str:"", int:0, bool:False, float:0.0, datetime:datetime(1, 1, 1)}
    _convertions_from_string = {str:lambda value:value, int:lambda value:int(value), bool:lambda value:bool(value),
                                float:lambda value:float(value), datetime:lambda value:datetime(value)}

    def __init__(self, _type):
        self.type = _type

    @property
    def type(self):
        return self.__type

    @type.setter
    def type(self, _type):
        if isinstance(_type, type):
            self.__type = _type
        elif isinstance(_type, str):
            type_str = _type.replace("<class ", "").replace(">", "").replace("'","")
            if type_str in self._types:
                self.__type = self._types[type_str]
            else:
                self.__type = None
        else:
            self.__type = None

    @property
    def default_value(self):
        if self.__type in self._default_values:
            return self._default_values[self.__type]
        else:
            return None

    def convert_from_string(self, string:str):
        return self._convertions_from_string[self.__type](string)

class Relation:

    #ПРИМЕЧАНИЕ - ДОБАВИТЬ МЕТОД __next__()
    #ПРИМЕЧАНИЕ - ЗАМЕНИТЬ None на значения по умолчанию для типа колонки. добавить проверку типизации и вообще
    #ПРИМЕЧАНИЕ - добавить сортировку
    #ПРИМЕЧАНИЕ - ПОСМОТРЕТЬ, можно ли обойтись без вложенных циклов и условий при обновлении аттрибутов
    #ПРИМЕЧАНИЕ - МОГУТ ПОТРЕБОВАТЬСЯ ДОПОЛНИТЕЛЬНЫЕ СВОЙСТВА КОЛОНОК (тогда изменить их с листа на новый класс)
    #ПРИМЕЧАНИЕ - БЫЛО БЫ НЕПЛОХО, ЕСЛИ БЫ У ДОЧЕРНИХ КЛАССОВ БЫЛА ВСПЛЫВАЮЩАЯ ПОДСКАЗКА ПО СТОЛБЦАМ
    #attributes to add are taken both from 'attributes' and '_attributes'. attributes from 'attributes' take precedence
    def __init__(self, attributes:dict = None, attributes_fixed:bool = False, records_fixed:bool = False, **_attributes):
        self.__attributes       = _relation_attributes(attributes, **_attributes)
        self.__records          = []
        self.__attributes_fixed = attributes_fixed
        self.__records_fixed    = records_fixed

    def __repr__(self):
        values = "\n".join([str(record) for record in self.__records])
        return f"\nclass:{self.__class__}\nattributes:{self.__attributes}\nValues:\n{values}"

    def __iter__(self):
        return iter(self.records)

    def __copy__(self):
        return copy.deepcopy(self)

    @property
    def attributes(self):
        return self.__attributes.copy()

    @property
    def attributes_names(self):
        return tuple(self.__attributes)

    @property
    def records(self):
        return self.__records.copy()

    @property
    def attributes_fixed(self):
        return self.__attributes_fixed

    @property
    def records_fixed(self):
        return self.__records_fixed

    @property
    def values(self):
        return [record.values for record in self]

    #!!Переписать изменение состава колонок строки с учетом изменения типа строки
    #А лучше - добавить метод изменения состава атрибутов  класс _Record
    #Написать юнит-тесты к модулю тренировки ради
    #добавить метод сортировки

    #ПРИМЕЧАНИЕ - ДОБАВИТЬ ИЗМЕНЕНИЕ ТИПА АТТРИБУТА
    #ПРИМЕЧАНИЕ - ДОБАВИТЬ ИЗМЕНЕНИЕ ПОРЯДКА АТТРИБУТОВ С УЧЕТОМ НЕОБХОДИМОСТИ МЕНЯТЬ ПОРЯДОК АТТРИБУТОВ В СТРОКАХ
    def attributes_add(self, attributes:dict, update_existing = False):
        #ПРИМЕЧАНИЕ -СДЕЛАТЬ поSOLIDнее - 2 класса с фиксированными колонками и без (чтобы лишние методы не болтались)
        #ПРИМЕЧАНИЕ - ПОСМОТРЕТЬ, как это правильно делается по SOLIDу (в закладках на мобиле вроде была статья с хабра)
        if self.__attributes_fixed:
            raise Exception("attributes in this relation are fixed!")

        if update_existing:
            self.__attributes.update(attributes)
        else:
            for i in attributes:
                if not self.__attributes.get(i):
                    self.__attributes[i] = attributes[i]
        if len(self.__records) > 0:
            for i in self.__records:
                i._change_atributes(attributes, update_existing)


    def attributes_drop(self, attribute_names:list):
        if self.__attributes_fixed:
            raise Exception("attributes in this relation are fixed!")

        for i in attribute_names:
            self.__attributes.pop(i, None)
        if len(self.__records) > 0:
            for i in self.__records:
                i._drop_atributes(attribute_names)

    #values to fill are taken both from 'values' and '_values'. values from 'values' take precedence
    def records_add(self, values:dict = None, **_values):
        if self.__records_fixed:
            raise Exception("records in this relation are fixed!")
        record = _Record(self.__attributes)
        if len(_values) > 0:
            record.fill(_values)
        if values:
            record.fill(values)
        self.__records.append(record)
        return record

    #record: int (index),
    def records_drop(self, record):
        if self.__records_fixed:
            raise Exception("records in this relation are fixed!")
        if isinstance(record, _Record):
            self.__records.remove(record)
        elif isinstance(record, int):
            self.__records.pop(record)
        else:
            raise Exception("Incorrect record - must be index or record object")

    def clear(self):
        if self.__records_fixed:
            raise Exception("records in this relation are fixed!")
        self.__records.clear()

    def count(self):
        return len(self.__records)

    def copy_attributes(self):
        return Relation(self.__attributes)

    def is_empty(self):
        return self.count() == 0

    def find_value(self, value, attributes:list = None) -> list:
        return [record for record in self if record.find_value(value, attributes)]

    #ПРИМЕЧАНИЕ - ДОБАВИТЬ МЕТОД СОРТИРОВКИ (ЗАОДНО С АЛГОРИТМАМИ СОРТИРОВКИ РАЗОБРАТЬСЯ)
    #ДОБАВИТЬ МЕТОД ДЛЯ КОПИРОВАНИЯ С ВОЗМОЖНОСТЬЮ ФИКСАЦИИ/РАСФИКСАЦИИ КОЛОНОК/СТРОК

#single record of a Relation
class _Record:
    # attributes to add are taken both from 'attributes' and '_attributes'. attributes from 'attributes' take precedence
    def __init__(self, attributes:dict, **_attributes):
        #do not change the next string (loook __setattr__)
        self.__attributes = _relation_attributes(attributes, **_attributes)
        self.__change_properties(self.__attributes.keys())

    def __repr__(self):
        result = {}
        for i in self.__attributes:
            result[i] = self.__getattribute__(i)
        return str(result)

    def __setattr__(self, key, value):
        #ПРИМЕЧАНИЕ - ПОТОМ РАЗОБРАТЬСЯ, КАК СДЕЛАТЬ МЕНЕЕ КОСТЫЛЬНО
        if key != "_Record__attributes" and key in self.__attributes:
            attribute_type = self.__attributes[key]
            self.__dict__[key] = self.__setted_value(attribute_type, value)
        else:
            self.__dict__[key] = value

    @property
    def attributes(self):
        return self.__attributes

    @property
    def values(self):
        return {attribute: self.get(attribute) for attribute in self.__attributes}

    def fill(self, values:dict):
        for i in values:
            if i in self.__dict__:
                self.__setattr__(i, values[i])

    #ПРИМЕЧАНИЕ - СЮДА МОЖНО ПОТОМ ПРОВЕРКИ НА КОРРЕКТНОСТЬ ИМЕНИ ПРИКРУТИТЬ
    def get(self, name):
        return self.__getattribute__(name)

    def set(self, name, value):
        self.__setattr__(key, value)

    def to_dict(self):
        return self.values

    def find_value(self, value, attributes:list = None):
        attributes_list = []
        if not attributes:
            search_attributes = list(self.attributes.keys())
        else:
            self.__check_search_attributes(attributes)
            search_attributes = attributes.copy()
        for attribute in search_attributes:
            if self.get(attribute) == value:
                attributes_list.append(attribute)
        return attributes_list

    def _change_atributes(self, attributes:dict, change_existing = False):
        if change_existing:
            self.__attributes.update(attributes)
        else:
            for attribute in attributes:
                if not attribute in self.__attributes:
                    self.__attributes[attribute] = attributes[attribute]
        self.__change_properties(attributes.keys(), change_existing)

    def _drop_atributes(self, attribute_names:list):
        for name in attribute_names:
            self.__attributes.pop(name, None)
        self.__change_properties(attribute_names)

    def __check_search_attributes(self, attributes:list):
        for attribute in attributes:
            if not attribute in self.__attributes:
                raise ValueError(f"There is no attribute {attribute} in record attributes:\n{self.__attributes}")

    def __change_properties(self, properties, change_existing=False):
        for property in properties:
            if not property in self.__dict__ or change_existing:
                self.__setattr__(property, None)

    def __setted_value(self, attribute_type, value):
        if not attribute_type:
            return value
        elif isinstance(attribute_type, tuple):
            return self.__setted_enumeration_value(attribute_type, value)
        else:
            return self.__setted_type_value(attribute_type, value)

    def __setted_enumeration_value(self, enumeration, value):
        if value == None:
            return enumeration[0]
        elif value in enumeration:
            return value
        else:
            raise ValueError(f"Value {value} isn't in {enumeration}!")

    def __setted_type_value(self, attribute_type, value):
        if value == None:
            return TypeManager(attribute_type).default_value
        elif isinstance(value, attribute_type):
            return value
        else:
            raise ValueError(f"Value {value} isn't {attribute_type}!")

#decorators
#ПРИМЕЧАНИЕ - ДОБАВИТЬ ПОДДЕРЖКУ РАЗНЫХ ФОРМАТОВ
def Serializer(function):
    def wrapped(*args, **kwargs):
        result = function(*args, **kwargs)
        return json.JSONEncoder().encode(result)
    return wrapped

#decorators
#ПРИМЕЧАНИЕ - ПОТОМ ПЕРЕПРОВЕРИТЬ МЕТОД: В ТЕКУЩЕМ ВИДЕ КРИВОВАТО ВЫГЛЯДИТ
#decorated function must return basic_entities.ResultDescription
def WithChangeSettings(filepath:str):
    def decorator(function):
        def wrapped(*args, **kwargs):
            file = pathlib.Path(filepath)
            if file.exists():
                #ПРИМЕЧАНИЕ - НАЙТИ НОРМАЛЬНУЮ ПРОВЕРКУ СВОЙСТВА READONLY
                if not "w" in stat.filemode(file.stat().st_mode):
                    error_descr = f"File {filepath} is read only. It may being changed by another process"
                    return ResultDescription(False, error_descr)
                file.chmod(stat.S_IREAD)
                result = function(*args, **kwargs)
                if not result.success:
                    file.chmod(stat.S_IWRITE)
                    return result
            filecopy = pathlib.Path(file.with_stem(f"__{file.stem}"))
            filecopy.write_text(result.result)
            if file.exists():
                file.chmod(stat.S_IWRITE)
                file.unlink()
            filecopy.rename(filepath)
            return result
        return wrapped
    return decorator

#data operations' settings

class OperationSet(Relation):
    #ПРИМЕЧАНИЕ - ПОТОМ КАК_ТО ОТРЕФАКТОРИТЬ ТАК, ЧТОБЫ ДЛЯ КАЖДОГО ТИПА ОПЕРАЦИИ БЫЛА ПОДСКАЗКА ПО Settings
    def __init__(self):
        attributes = {"Type":data_operations, "Table":str, "Settings":dict, "Result":None}
        super().__init__(attributes)

class OperationFilter(Relation):
    #ПРИМЕЧАНИЕ - ГДЕ-ТО ДОЛЖНА БЫТЬ ПРОВЕРКА, ЧТО ПОСЛЕДНЯЯ СТРОКА НЕ ДОЛЖНА ИМЕТЬ ОПЕРАЦИИ, А ОСТАЛЬНЫЕ - ДОЛЖНЫ ОБЯЗАТЕЛЬНО
    #ПРИМЕЧАНИЕ - А ВООБЩЕ ЭТО ВРЕМЯНКА. ПОТОМ СДЕЛАТЬ ОПИСАНИЕ ФИЛЬТРА ПОЛУЧШЕ - С ПОДДЕРЖКОЙ ГРУППИРОВКИ УСЛОВИЙ
    def __init__(self):
        attributes = {"Attribute":str, "Compare":("=", ">", "<", "<>", "LIKE"), "Value":None, "Operation":("AND", "OR", "")}
        super().__init__(attributes, True)

class OperationResultOrder(Relation):
    def __init__(self):
        attributes = {"Attribute":str, "Way":("ASC", "DESC")}
        super().__init__(attributes, True)

class DBRelationDescription(Relation):
    def __init__(self):
        super().__init__(_attributes_settings, True)

    def descripted_relation(self):
        return DescriptedRelation(self)

class DescriptedRelation(Relation):
    def __init__(self, description:DBRelationDescription):
        attributes = {attribute.Name:attribute.Type for attribute in description}
        super().__init__(attributes, True)

class RelationAttributeDescription(_Record):
    def __init__(self, type_is_str:bool = False, **values):
        attributes_settings = _attributes_settings.copy()
        attributes_settings.pop("Primary_key")
        if type_is_str:
            attributes_settings["Type"] = str
        super().__init__(attributes_settings)
        super().fill(values)

class RelationIndexDescription:
    # ПРИМЕЧАНИЕ - ПРИКРУТИТЬ ВЕЗДЕ, ГДЕ НУЖНО (А ГДЕ НУЖНО? - ОПРЕДЕЛИТЬ) ПРОВЕРКИ ТИПОВ ЗНАЧЕНИЙ ПАРАМЕТРОВ? ВРОДЕ ЕСТЬ БИБЛИОТЕКА ПОД ЭТО ДЕЛО
    def __init__(self, name: str, attributes: list, cluster: bool = False, **kwargs):
        self.__check_attributes(attributes)
        self.__name = name
        self.__attributes = attributes
        self.__cluster = cluster

    def __repr__(self):
        return f"Name:{self.name}\nAttributes:{self.attributes}\nCluster:{self.cluster}"

    @property
    def name(self):
        return self.__name

    @property
    def attributes(self):
        return self.__attributes.copy()

    @property
    def cluster(self):
        return self.__cluster

    # service
    def __check_attributes(self, attributes: tuple):
        if not isinstance(attributes, list):
            raise TypeError(f"Index attributes {attributes} is not list!")
        for attribute in attributes:
            if not isinstance(attribute, str):
                raise ValueError(f"Index attribute {attribute} is not str!")

class IDGenerator():
    @staticmethod
    def generate():
        return str(uuid.uuid4())

#ПРИМЕЧАНИЕ - ПРОВЕРИТЬ, МОЖНО ЛИ ИЗМЕНИТЬ ЗНАЧЕНИЯ ГЛОБАЛЬНЫХ ПЕРЕМЕННЫХ ИЗ КОДА И К КАКИМ ПОСЛЕДСТВИЯМ ЭТО ПРИВЕДЕТ
#ПРИМЕЧАНИЕ - ЕСЛИ МОЖНО И ЭТО ВСЕ ЛОМАЕТ - ЗАЩИТИТЬ ОТ ИЗМНЕНИЙ, НЕ МЕНЯЯ ИМЕНИ И ТИПА
data_operations = ("create", "rename", "drop", "add_attribute", "drop_attribute", "alter_attribute", "select", "insert", "update", "delete", "index",
                   "drop_index")
_attributes_settings = {"Name":str, "Type":type, "Primary_key":bool, "Not_null":bool, "Default":None}
_attributes_operations = ("create", "drop", "alter")










