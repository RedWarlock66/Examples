from abc import ABC, abstractmethod
from json import JSONEncoder, JSONDecoder
import copy
from information_service.basics import TypeManager, IDGenerator, Owner, Numerator, ObjectLink
import re

class MDObjectsTypes:
    reference = "reference"

class MDPropertyDescriptor:

    #to use note_changes property owner should have the "_changes" list for checking changes
    #to use notify_changes and/or check_unique property owner the "owner" property which is instance of class
    #which has notify_child_changes(child_object, changes_exist:bool) and/or check_unique(child_object, property:str, value) methods
    #check_unique should raise an exception if not passed
    def __init__(self, name:str = None, value_type:type = None, read_only:bool = False, check_function = None,
                 note_changes:bool = False, notify_changes:bool = False, check_unique:bool = False):
        if not name is None:
            self.__name = name
        self.__type = value_type
        self.__check_function = check_function
        self.__read_only = read_only
        self.__note_changes = note_changes or notify_changes
        self.__notify_changes = notify_changes
        self.__check_unique = check_unique
        self.__initiated = []  # list of instances in which the property is initiated
        self.__initial_values = {}  # dict of initial values of the property in each instance

    def __set__(self, instance, value):
        if self.__read_only and instance in self.__initiated:
           raise Exception(f"Property {self.__name} is read only!")
        self.__check_value(instance, value)
        instance.__dict__[self.__name] = value
        if not instance in self.__initiated:
            self.__initiated.append(instance)
            self.__initial_values[instance] = value
        elif self.__note_changes:
            self.__note_change(instance, value)

    def __get__(self, instance, owner):
        return instance.__dict__[self.__name]

    def __set_name__(self, owner, name):
        self.__name = name

    @property
    def type(self):
        return self.__type

    def __check_value(self, instance, value):
        if not self.__check_function is None:
            self.__check_function(value)
        elif not self.__type is None and not isinstance(value, self.__type):
            raise TypeError(f"Property {self.__name} should be an instance of {self.__type}")
        if self.__check_unique:
            instance.owner._check_unique(instance, self.__name, value)

    def __note_change(self, instance, value):
        if self.__notify_changes:
            was_unchanged = not instance.changed

        if value == self.__initial_values[instance] and self.__name in instance._changes:
            instance._changes.remove(self.__name)
        elif value != self.__initial_values[instance] and not self.__name in instance._changes:
            instance._changes.append(self.__name)

        if self.__notify_changes:
            unchanged = not instance.changed
            if unchanged != was_unchanged:
                instance.owner._notify_child_changes(instance, not unchanged)

class MDEntity(ABC):

    #ПРИМЕЧАНИЕ - БЕЗ АБСТРАКТНЫХ МЕТОДОВ АБСТРАКТИТЬСЯ НЕ ХОЧЕТ. ПОТОМ ПОСМОТРЕТЬ, ЧТО С ЭТИМ МОЖНО СДЕЛАТЬ

    #ПРИМЕЧАНИЕ - ЧТОБЫ НЕ ГЛЮЧИЛО, НУЖНО ПЕРЕОПРЕДЕЛЯТЬ ДВА СВОЙСТВА НИЖЕ В КАЖДОМ ДОЧЕРНЕМ КЛАССЕ. ПОТОМ ПЕРЕПИЛИТЬ
    #РАБОТУ С НИМИ НА АБСТРАКТНЫЕ СВОЙСТВА
    _properties =  {"_id": str, "store_name": str, "name": str}
    _custom_properties = []

    @staticmethod
    def __check_name(value):
        if not isinstance(value, str):
            raise ValueError(f"Name {value} is not string!")
        if not value:
            raise ValueError("Name cannot be empty!")
        if not value[0].isalpha() and value[0] != "_":
            raise ValueError(f"First symbol of {value} is not letter or _!")
        pattern = r"^[a-zA-Z0-9_]+$"
        # примечание - на \n регулярки почему-то не реагируют. потом разобраться, почему
        if not re.compile(pattern).search(value) or value.find("\n") > -1:
            raise ValueError(f"One of the symbols of {value} doesn't match {pattern}")

    owner = MDPropertyDescriptor(value_type=Owner,read_only=True)
    _id = MDPropertyDescriptor(value_type=str,read_only=True)
    store_name = MDPropertyDescriptor(value_type=str, read_only=True, check_function=__check_name)
    is_new = MDPropertyDescriptor(value_type=bool, read_only=True)
    deletion_mark = MDPropertyDescriptor(value_type=bool, notify_changes=True)
    name = MDPropertyDescriptor(check_function=__check_name, notify_changes=True, check_unique=True)

    def __init_subclass__(cls, **custom_properties):
        for name, value in custom_properties.items():
            if name in MDEntity._properties:
                raise Exception(f"There is standard property named {name}. Choose another name for custom property")
            setattr(cls, name, value)
            if not name in cls._properties:
                cls._properties[name] = value.type
            cls._custom_properties.append(name)
        super().__init_subclass__()

    def __init__(self, owner:Owner, _id:str, store_name:str, name:str, is_new:bool = False, **custom_properties):
        self._changes = []
        self.owner = owner
        self._id = _id
        self.store_name = store_name
        self.name = name
        self.is_new = is_new
        self.deletion_mark = False
        for custom_property in self._custom_properties:
            if custom_property in custom_properties:
                setattr(self, custom_property, custom_properties[custom_property])
            elif not custom_property in self.__dict__:
                setattr(self, custom_property, TypeManager.default(self._properties[custom_property]))

    def __repr__(self):
        return f"\n{self.__class__}:\n{self.properties_values()}"

    def properties_values(self, changed_only:bool = False, encoded:bool = False) -> dict:
        return {name:self.__property_value(name, encoded) for name in self._properties
                if not changed_only or name in self._changes}

    def __property_value(self, _property, encoded):
        if encoded:
            value = self.get_encoded_value(_property)
        else:
            value = getattr(self, _property)
        return copy.deepcopy(value)

    @property
    def changed(self):
        return not not self._changes

    def get_encoded_value(self, _property):
        return getattr(self, _property)

    def set_encoded_value(self, _property, value):
        setattr(self, _property, self.decode_value(_property, value))

    @classmethod
    def decode_value(cls, _property, value):
        return value

class MDAttribute(MDEntity, object_id = MDPropertyDescriptor(name = "object_id", value_type=str, read_only=True),
                            value_type = MDPropertyDescriptor(name="value_type", value_type=type, notify_changes=True,
                                                              check_function=TypeManager.is_allowed),
                            default = MDPropertyDescriptor(name="default", notify_changes=True)):

    object_id:str
    value_type: type
    default:None

    #ПРИМЕЧАНИЕ - ПОТОМ ПРОВЕРИТЬ, БУДЕТ ЛИ КОРРЕКТНО ОТРАБАТЫВАТЬ, ЕСЛИ ДОБАВЛЯТЬ СВОЙСТВА ЧЕРЕЗ new ИЛИ init
    #ПРИМЕЧАНИЕ - НЕ ОЧЕНЬ АККУРАТНО, НО ПУСТЬ БУДЕТ. ИЛИ АККУРАТНЕЕ БУДЕТ ЯВНО ПРОПИСАТЬ ВСЕ СВОЙСТВА ТУТ?
    _properties = {"_id": str, "object_id": str, "store_name": str, "name": str, "value_type":str, "default":str}
    _custom_properties = []

    def __setattr__(self, key, value):
        if key == "default" and not TypeManager.istype(value, self.value_type):
            raise ValueError(f"Default value should be {self.value_type}")
        elif key == "default" and isinstance(self.value_type, ObjectLink) and value != "":
            raise ValueError("ObjectLink default value cannot be changed")
        MDEntity.__setattr__(self, key, value)
        if key == "value_type":
            MDEntity.__setattr__(self, "default", TypeManager.default(self.value_type))
            if key in self._changes and not "default" in self._changes:
                #have to forcibly note changing of default due to default int (0) == default float (0.0)
                self._changes.append("default")

    @property
    def changed(self):
        return super().changed

    def get_encoded_value(self, _property):
        if _property == "value_type":
            return TypeManager.type_to_string(self.value_type)
        elif _property == "default":
            return JSONEncoder().encode(self.default) #здесь нужен вызов TypeManager (есть специфические типы)
        else:
            return MDEntity.get_encoded_value(self, _property)

    def set_encoded_value(self, _property, value):
        MDEntity.set_encoded_value(self, _property, value)

    @classmethod
    def decode_value(cls, _property, value):
        if _property == "value_type":
            return TypeManager.string_to_type(value)
        elif _property == "default":
            return JSONDecoder().decode(value)
        else:
            return value

class MDIndex(MDEntity, object_id = MDPropertyDescriptor(name = "object_id", value_type=str, read_only=True),
                        attributes = MDPropertyDescriptor(name="attributes", value_type=tuple,read_only=True),
                        cluster = MDPropertyDescriptor(name="cluster", value_type=bool, read_only=True)):
    object_id:str
    attributes:tuple #Tuple of MDAttributes' names
    cluster:bool

    _properties = {'_id':str, 'store_name':str, 'name':str, 'object_id':str, 'attributes':str, 'cluster':bool}
    #ПРИМЕЧАНИЕ - МЕХАНИКА ХРАНЕНИЯ АТТРИБУТОВ НЕ ОЧЕНЬ АККУРАТНАЯ. ПОТОМ ОПТИМИЗИРОВАТЬ

    def get_encoded_value(self, _property):
        if _property == "attributes":
            return JSONEncoder().encode(self.attributes)
        else:
            return MDEntity.get_encoded_value(self, _property)

    def set_encoded_value(self, _property, value):
        #use only to construct indexes, which are readed from database
        MDEntity.set_encoded_value(self, _property, value)

    @classmethod
    def decode_value(cls, _property, value):
        if _property == "attributes":
            return tuple(JSONDecoder().decode(value))
        else:
            return value

class MDEntityCollection(Owner):
    def __init__(self, value_class, store_prefix:str = "", owner:Owner = None, **common_properties):
        if not issubclass(value_class, MDEntity):
            raise TypeError("Value type should be subclass of MDEntity")
        self.__owner = owner
        self.__value_class = value_class
        self.__elements = []
        self.__changed_elements = []
        self.__store_prefix = store_prefix
        self.__common_properties = common_properties

    def __repr__(self):
        return "\n".join([str(element.properties_values()) for element in self.__elements])

    def __iter__(self):
        return iter(self.elements())

    def __getitem__(self, item):
        return self.__elements[item]

    @property
    def owner(self):
        return self.__owner

    @property
    def property_name(self):
        return self.__property_name

    @property
    def value_class(self):
        return self.__value_class

    @property
    def changed(self):
        return not not self.__changed_elements

    def add_element(self, _id:str = None, store_name:str = None, is_new = False, value_class = None, **properties) -> MDEntity:
        #ПРИМЕЧАНИЕ - ПОТОМ ПРИКРУТИТЬ ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ ДЛЯ НЕ УКАЗАННЫХ АТРИБУТОВ (пример - cluster у MDIndex)
        properties_values = properties.copy()
        properties_values.update(self.__common_properties)
        if value_class:
            if not issubclass(value_class, self.__value_class):
                raise ValueError(f"Value class should be subclass of {self.__value_class}")
        else:
            value_class = self.__value_class
        if not _id:
            _id = IDGenerator.generate()
        if not store_name:
            store_name = Numerator.next_numerated_value(list(self.__elements), "store_name", self.__store_prefix)
        element = value_class(owner=self, _id=_id, store_name=store_name, is_new=is_new, **properties_values)
        self.__elements.append(element)
        if is_new:
            self.__note_change(element, True)
        return element

    def get_element(self, _property, value):
        for element in self.__elements:
            if getattr(element, _property) == value:
                return element
        return None

    def set_deletion_mark(self, element:MDEntity, deletion_mark:bool = True):
        element.deletion_mark = deletion_mark
        if element.is_new:
            self.__note_change(element, not deletion_mark)

    def elements(self, changed_only:bool = False) -> list:
        if changed_only:
            return self.__changed_elements
        else:
            return self.__elements

    def clear(self):
        self.__elements.clear()
        self.__changed_elements.clear()

    def _notify_child_changes(self, element, changes_exist:bool):
        element_is_changed = (element.is_new and not element.deletion_mark) or (not element.is_new and changes_exist)

        self.__note_change(element, element_is_changed)

    def _check_unique(self, child_object, prop:str, value):
        for element in self.__elements:
            if getattr(element, prop) == value and not element == child_object:
                raise ValueError(f"There is another element with property {prop} == {value}. Choose another name")

    def __note_change(self, element:MDEntity, changed:bool):
        was_changed = self.changed
        if changed and not element in self.__changed_elements:
            self.__changed_elements.append(element)
        elif not changed and element in self.__changed_elements:
            self.__changed_elements.remove(element)
        is_changed = self.changed
        if not is_changed == was_changed and self.__owner:
            self.__owner._notify_child_changes(self, is_changed)

class MDObject(MDEntity, md_type = MDPropertyDescriptor(name="md_type", value_type=str, read_only=True)):

    md_type:str
    _properties = {"_id": str, "store_name": str, "name": str, "md_type": str}
    _custom_properties = []

    def __init__(self, owner:Owner, _id:str, store_name:str, name:str, md_type:str, is_new:bool = False):
        index_prefix = f"{store_name}_ind"
        MDEntity.__init__(self, owner=owner, _id=_id, store_name = store_name, name=name, is_new=is_new, md_type=md_type)
        self.__attributes = MDEntityCollection(owner=self, value_class=MDAttribute, store_prefix="_attr", object_id=self._id)
        self.__indexes = MDEntityCollection(owner=self, value_class=MDIndex, store_prefix=index_prefix, object_id=self._id)
        self.__changed_collections = []

    def __repr__(self):
        return f"{MDEntity.__repr__(self)}\nattributes:\n{self.__attributes}\nindexes:\n{self.__indexes}"

    def __getitem__(self, attribute_name) -> MDAttribute:
        attribute = self.attributes.get_element("name", attribute_name)
        if attribute is None:
            attribute = self.standard_attributes.get_element("name", attribute_name)
            if attribute is None:
                raise KeyError(f"There is no attribute name {attribute_name}")
            else:
                return copy.deepcopy(attribute)
        else:
            return attribute

    def get_attribute(self, _property, value):
        for attribute in self.attributes:
            if getattr(attribute, _property) == value:
                return attribute
        return None

    @property
    def changed(self):
        return super().changed or not not self.__changed_collections

    @property
    def attributes(self) -> MDEntityCollection:
        return self.__attributes

    @property
    def indexes(self) -> MDEntityCollection:
        return self.__indexes

    def add_attribute(self, name:str, value_type:type = str, default = "", is_new:bool = True, _id:str = None,
                      store_name:str = None) -> MDAttribute:
        if not default:
            default = TypeManager.default(value_type)
        if self.is_new:
            is_new = True
        return self.__attributes.add_element(name=name, value_type=value_type, default=default, is_new=is_new, _id=_id,
                                             store_name=store_name)

    def add_index(self, attributes:tuple, cluster:bool = False, is_new:bool = True, _id:str = None,
                      store_name:str = None, name:str = None) -> MDIndex:
        if name is None:
            index_name = self.__index_name(attributes)
        else:
            index_name = name
        if self.is_new:
            is_new = True
        return self.__indexes.add_element(name=index_name, attributes=attributes, cluster=cluster, is_new=is_new,_id=_id,
                                             store_name=store_name)

    def drop_attribute(self, attribute:MDAttribute):
        self.__attributes.set_deletion_mark(attribute, True)

    def drop_index(self, index:MDIndex):
        self.__indexes.set_deletion_mark(index, True)

    def link(self):
        #ПРИМЕЧАНИЕ - БУДЕТ РАБОТАТЬ ТОЛЬКО ЕСЛИ КОНЕЧНЫЙ ВЛАДЕЛЕЦ - Metadata. ДРУГОГО ПОКА НЕ ПРЕДПОЛАГАЕТСЯ, ТАК ЧТО
        #ПРОВЕРКИ ПОКА ДОБАВЛЯТЬ НЕ БУДЕМ
        return self.owner.owner.get_object_link(self)

    @classmethod
    @property
    @abstractmethod
    def standard_attributes(cls) -> MDEntityCollection:
        pass

    @classmethod
    @property
    @abstractmethod
    def standard_indexes(cls) -> MDEntityCollection:
        pass

    def _notify_child_changes(self, child_object, changes_exist:bool):
        was_changed = self.changed
        if changes_exist and not child_object in self.__changed_collections:
            self.__changed_collections.append(child_object)
        elif not changes_exist and child_object in self.__changed_collections:
            self.__changed_collections.remove(child_object)
        is_changed = self.changed
        if is_changed != was_changed:
            self.owner._notify_child_changes(self, is_changed)

    def __index_name(self, attributes:tuple) -> str:
        index_name = []
        for attribute in attributes:
            self.__check_attribute_name(attribute)
            index_name.append(attribute)
        return '_'.join(index_name)

    def __check_attribute_name(self, attribute_name):
        name_exist = False
        for attribute in self.__attributes:
            if attribute_name == attribute.name and not attribute.deletion_mark:
                name_exist = True
        if not name_exist:
            raise ValueError(f"Index attribute {attribute_name} doesn't belong this object")

class MDObjectReference(MDObject):

    __standard_attributes = MDEntityCollection(value_class=MDAttribute, store_prefix="")
    __standard_attributes.add_element(store_name="_id", is_new=False, name="id", value_type=str)
    __standard_attributes.add_element(store_name="_parent_id", is_new=False, name="parent_id",
                                              value_type=str)
    __standard_attributes.add_element(store_name="_name", is_new=False, name="name", value_type=str)
    __standard_attributes.add_element(store_name="_is_folder", is_new=False, name="is_folder",
                                              value_type=bool)
    __standard_attributes.add_element(store_name="_del_mark", is_new=False, name="del_mark",
                                              value_type=bool)

    def __init__(self, owner:Owner, _id:str, store_name:str, name:str, is_new:bool = False):
        MDObject.__init__(self, owner=owner, _id=_id, store_name=store_name, name=name, md_type=MDObjectsTypes.reference, is_new=is_new)
        self.__standard_indexes = MDEntityCollection(value_class=MDIndex, store_prefix="")
        self.__standard_indexes.add_element(store_name=f"{store_name}_id", store_prefix="", name="id",
                                       attributes=("id",), cluster=True, is_new=False)
        self.__standard_indexes.add_element(store_name=f"{store_name}_parent", store_prefix="", name="parent_id",
                                       attributes=("parent_id", "id"), cluster=False, is_new=False)

    def __repr__(self):
        return f"{MDObject.__repr__(self)}\nstandard_attributes:\n{self.__standard_attributes}" \
               f"\nstandard_indexes\n{self.__standard_indexes}"

    @classmethod
    @property
    def standard_attributes(cls):
        return copy.deepcopy(cls.__standard_attributes)

    @property
    def standard_indexes(self):
        return copy.deepcopy(self.__standard_indexes)

    def add_attribute(self, name:str, value_type:type = str, default = "", is_new:bool = True, _id:str = None,
                      store_name:str = None) -> MDAttribute:
        for standard_attribute in self.__standard_attributes:
            if standard_attribute.name == name:
                raise ValueError(f"There is standard attribute named {name}! Choose another name")
        return MDObject.add_attribute(self, name, value_type, default, is_new, _id=_id, store_name=store_name)

class _EntityTablesDescription:
    def __init__(self, add_to:dict, delete_from:dict):
        '''add_to, delete_from: keys - metadata tables' names, values - metadata tables' id storing columns'''
        self.__add_to = add_to
        self.__delete_from = delete_from

    @property
    def add_to(self):
        return self.__add_to

    @property
    def delete_from(self):
        return self.__delete_from

class _EntitiesTablesDescription:

    def __init__(self):
        self.__entity_tables_descriptions = {}

    def __setitem__(self, key, value):
        if not issubclass(key, MDEntity):
            raise TypeError(f"{key} is not subclass of MDEntity")
        if not isinstance(value, _EntityTablesDescription):
            raise TypeError(f"{value} is not instance of _EntityTablesDescription")
        self.__entity_tables_descriptions[key] = value

    def __getitem__(self, key) -> _EntityTablesDescription:
        for _entity_class, tables_description in self.__entity_tables_descriptions.items():
            if isinstance(key, _entity_class):
                return tables_description
        raise KeyError(f"{key} is not instance of any class of _EntitiesTablesDescription")



