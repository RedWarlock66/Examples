#module for metadata objects
from abc import ABC, abstractmethod
from json import JSONEncoder, JSONDecoder
import datetime, uuid, re

class ObjectLink:
    def __init__(self, objects_id):
        if isinstance(objects_id, str):
            self.__objects = [objects_id]
        elif isinstance(objects_id, list):
            self.__objects = []
            for object_id in objects_id:
                self.__check_object_id(object_id)
                self.__objects.append(object_id)
        else:
            raise TypeError(f"Object id {objects_id} is not str!")

    def __repr__(self):
        return "ObjectLink:\nobjects' id:" + ", ".join(self.__objects)

    @property
    def objects(self):
        return self.__objects

    def belongs(self, object_id:str):
        return object_id in self.__objects

    def add_object(self, object_id:str):
        self.__check_object_id(object_id)
        if not object_id in self.__objects:
            self.__objects.append(object_id)

    def remove_object(self, object_id:str):
        if object_id in self.__objects:
            self.__objects.remove(object_id)

    def encoded_link(self, object_id:str, _id:str):
        if not self.belongs(object_id):
            raise ValueError(f"Object {object_id} doesn't belong {self.__objects}")
        _link = {"object_id":object_id, "id":_id}
        return JSONEncoder().encode(_link)

    @staticmethod
    def decoded_link(link:str):
        return JSONDecoder().decode(link)

    def __check_object_id(self, object_id):
        if not isinstance(object_id, str):
            raise TypeError(f"Object id {object_id} is not str!")

class TypeManager(ABC):

    __allowed_types = {str:"", bool:False, int:0, float:0.0, datetime.datetime:datetime.datetime(1, 1, 1),
                       ObjectLink:""}
    _types_to_string = {str:"str", bool:"bool", int:"int", float:"float",
                        datetime.datetime:"datetime", ObjectLink:"ObjectLink"}
    _strings_to_type = {value:key for key, value in _types_to_string.items()}

    @classmethod
    def is_allowed(cls, value_type):
        return isinstance(value_type, ObjectLink) or (value_type in cls.__allowed_types and value_type != ObjectLink)

    @classmethod
    def default(cls, value_type):
        if isinstance(value_type, ObjectLink):
            return cls.__allowed_types[ObjectLink]
        elif cls.is_allowed(value_type):
            return cls.__allowed_types[value_type]
        elif value_type == type:
            return str
        else:
            return None

    @classmethod
    def allowed_types(cls):
        return list(cls.__allowed_types)

    @classmethod
    def check_value(cls, value, value_type = None, is_name:bool = False):
        if is_name:
            cls.__check_name(value)
        elif not cls.is_allowed(value_type):
            raise TypeError(f"Type {value_type} is not one of the allowed {cls.__allowed_types}")
        elif not isinstance(value, value_type):
            raise TypeError(f"Value {value} is not {value_type}")

    @classmethod
    def istype(cls, value, _type):
        if isinstance(_type, ObjectLink):
            #ПРИМЕЧАНИЕ - МОЖНО УЖЕСТОЧИТЬ ПРОВЕРКУ ДО УИДОВ
            return isinstance(value, str)
        else:
            return isinstance(value, _type)

    @classmethod
    def type_to_string(cls, _type) -> str:
        if not cls.is_allowed(_type):
            raise TypeError(f"Type {_type} is not supported!")
        return cls.__type_repr(_type)

    @classmethod
    def string_to_type(cls, str_type:str):
        repr_dict = JSONDecoder().decode(str_type)
        if cls._strings_to_type[repr_dict["type"]] == ObjectLink:
            return ObjectLink(repr_dict["objects"])
        else:
            return cls._strings_to_type[repr_dict["type"]]

    @staticmethod
    def __check_name(value):
        if not isinstance(value, str):
            raise ValueError(f"Name {value} is not string!")
        if not value:
            raise ValueError("Name cannot be empty!")
        if not value[0].isalpha():
            raise ValueError(f"First symbol of {value} is not letter!")
        pattern = r"^[a-zA-Z0-9_]+$"
        # примечание - на \n регулярки почему-то не реагируют. потом разобраться, почему
        if not re.compile(pattern).search(value) or value.find("\n") > -1:
            raise ValueError(f"One of the symbols of {value} doesn't match {pattern}")

    @classmethod
    def __type_repr(cls, _type):
        if isinstance(_type, ObjectLink):
            repr_dict = {"type":cls._types_to_string[ObjectLink], "objects":_type.objects}
        else:
            repr_dict = {"type":cls._types_to_string[_type]}
        return JSONEncoder().encode(repr_dict)

class IDGenerator(ABC):
    @staticmethod
    def generate():
        return str(uuid.uuid4())

class Owner(ABC):

    @abstractmethod
    def _notify_child_changes(self, child_object, changes_exist:bool):
        pass

    @abstractmethod
    def _check_unique(self, child_object, prop:str, value):
        pass

class Serializer(ABC):

    @staticmethod
    def serialize(value):
        #ПРИМЕЧАНИЕ - ТУТ НУЖНА СЕРИАЛИЗАЦИЯ ДЛЯ НЕПРИМИТИВНЫХ ТИПОВ (ВРОДЕ RelationLink)
        return JSONEncoder().encode(value)

    @staticmethod
    def deseriazlize(text:str):
        #ПРИМЕЧАНИЕ - ТУТ НУЖНА ДЕСЕРИАЛИЗАЦИЯ ДЛЯ НЕПРИМИТИВНЫХ ТИПОВ (ВРОДЕ RelationLink)
        return JSONDecoder().decode(text)

class Numerator(ABC):

    @staticmethod
    def next_numerated_value(objects:list, property_name:str, prefix:str) -> str:
        #returns string [prefix[0-9]] where is max of [0-9] in property names of objects in object
        if not objects:
            return f"{prefix}1"
        #more readable than by using generator
        max_number = 0
        for object in objects:
            property_repr = str(getattr(object, property_name))
            if prefix in property_repr:
                current_number = property_repr.replace(prefix, "")
                if current_number.isdigit() and int(current_number) > max_number:
                    max_number = int(current_number)
        return f"{prefix}{max_number+1}"

class Collection(list):
    def __init__(self, value_type:type):
        self.__type = value_type

    def append(self, element) -> None:
        self.__check_new_element(element)
        super().append(element)

    def insert(self, index, element) -> None:
        self.__check_new_element(element)
        super().insert(index, element)

    def extend(self, collection) -> None:
        if not isinstance(collection, list) and not isinstance(collection, tuple):
            raise TypeError("Added collection is not list or tuple!")
        for element in collection:
            self.__check_new_element(element)
        super().extend(collection)

    def __check_new_element(self, element):
        if not isinstance(element, self.__type):
            raise TypeError(f"Element {element} isn't instance of {self.__type}")
        if element in self:
            raise ValueError(f"There already is element which has the value {element}")