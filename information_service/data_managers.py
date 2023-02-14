#module for different data management objects
import datetime

import InfoMaster.basic_entities as InfoMasterBasic, InfoMaster.dms_integration.integration as InfoMasterIntegation
import json, information_service.md_objects as md_objects
import os, pathlib
from abc import ABC, abstractmethod
from information_service.md_objects import MDObject, MDAttribute, MDIndex, MDEntityCollection, _EntitiesTablesDescription

#global variables
_infobase_settings_file = pathlib.Path(os.path.dirname(__file__), "config", "infobase_settings.cfg")

#abstract infobase manager which shoul be inherited by any infobase manager which information service uses
class InfobaseManager(ABC):
    @abstractmethod
    def create_infobase(self, name:str, dms:str, **kwargs):
        pass

class InfoMasterInfobaseManager(InfobaseManager):
    def create_infobase(self, name:str, dms:str, **kwargs):
        result = InfoMasterIntegation.InfobaseList().create(name=name, dms=dms, db_name=name, **kwargs)
        if not result.success:
            raise Exception(result.result)

#abstract data manager which should be inherited by any data manager which infromation service uses
class DataManager(ABC):
    @abstractmethod
    def __init__(self, infobase_name:str, settings:dict):
        self._infobase_name = infobase_name
        self._settings = settings

    @abstractmethod
    def create_metadata_storage(self, description:dict):
        '''description keys - metadata storage relations' names, values - dict (keys - attributes' names, values - attributes' types)'''
        pass

    @abstractmethod
    def read_metadata(self, metadata_object):
        '''storage_description keys - names of metadata storage tables, values - metadata classes'''
        '''object_keys keys - subclasses of MDEntity, values - names of fields which contain object id'''
        pass

    @abstractmethod
    def save_changes(self, objects:list, md_tables_description:_EntitiesTablesDescription):
        '''objects - list of instances of child classes of md_objects.MDObject'''

    @abstractmethod
    def read_data(self, storage:str, filter:dict = None, transaction:bool = False, direct_storage_name:bool = False) -> list:
        pass

    @abstractmethod
    def write_data(self, storage:str, data:list, filter:dict = None, direct_storage_name:bool = False):
        '''data - list of data_objects._Data_Record'''
        pass

#data manager for InfoMaster data management system
class InfoMasterDataManager(DataManager):

    _name_prefix = "_"
    _store_types = [str, bool, int, float, datetime.datetime] #точно на этом уровне абстракций?

    def __init__(self, infobase_name:str, settings:dict):
        super().__init__(infobase_name, settings)
        self.__infobase = InfoMasterIntegation.InfobaseList().get_infobase(self._infobase_name)

    def create_metadata_storage(self, description:dict):
        operations = InfoMasterBasic.OperationSet()
        for table, attributes in description.items():
            _table = self.__add_name_prefix(table)
            table_description = self.__table_description(attributes=attributes)
            operations.records_add(Type = "create", Table = _table, Settings = {"description":table_description})
        result = self.__infobase.execute_operations(operations, True)
        if not result.success:
            raise Exception(result.result)

    def read_metadata(self, metadata_object):
        #reading algorithm isn't very optimal but more readable and maintainable than more optimal algorithms
        #and should still work good with data amount used for prototype's purposes
        storage_data = self.__read_metadata_from_storage(metadata_object._metadata_objects)
        if storage_data[MDObject].Result:
            for object_data in storage_data[MDObject].Result:
                obj = metadata_object.add_object(is_new=False, **self.__creating_arguments(object_data, MDObject))
                self.__add_subobjects(object_data, MDAttribute, storage_data, metadata_object._metadata_object_keys,
                                      obj.add_attribute)
                self.__add_subobjects(object_data, MDIndex, storage_data, metadata_object._metadata_object_keys,
                                      obj.add_index)

    def save_changes(self, objects:list, md_tables_description:_EntitiesTablesDescription):
        #ПРИМЕЧАНИЕ - ПОТОМ ПОДУМАТЬ, ЗАПИСЫВАТЬ ЛИ ВСЕ ИЗМЕНЕНИЯ В ОДНОЙ ТРАНЗАКЦИИ ИЛИ В ОДНОЙ ТРАНЗАКЦИИ НА КАЖДЫЙ ОБЪЕКТ
        #пока будет в одной на все
        #ПРИМЕЧАНИЕ - ПОТОМ КАК-НИБУДЬ ОТРЕФАКТОРИТЬ ЭТО ДОБРО. ЕСТЬ ПОВТОРЫ (ИЗМЕНЕНИЕ АТРИБУТОВ, НАПРИМЕР)
        operations = InfoMasterBasic.OperationSet()
        for md_object in objects:
            self.__add_object_changes(operations, md_object, md_tables_description)
        result = self.__infobase.execute_operations(operations, True)
        if not result.success:
            raise Exception(result.result)

    def read_data(self, storage:str, filter:dict = None, transaction:bool = False, direct_storage_name:bool = False) -> list:
        _storage = self.__storage_name(storage, direct_storage_name)
        _filter  = self.__dict_to_filter(filter, direct_names=direct_storage_name)
        operations = InfoMasterBasic.OperationSet()
        operations.records_add(Type="select", Table=_storage, Settings={"filter":_filter})
        result = self.__infobase.execute_operations(operations, transaction)
        if not result.success:
            raise Exception(result.result)
        return [{self.__storage_name(field, direct_storage_name, True):value for field, value in record.values.items()}
                for record in operations.records[0].Result.records]

    def write_data(self, storage:str, data:list, filter:dict = None, direct_storage_name:bool = False):
        _storage = self.__storage_name(storage, direct_storage_name)
        _filter = self.__dict_to_filter(filter, direct_names=direct_storage_name)
        operations = InfoMasterBasic.OperationSet()
        operations.records_add(Type="delete", Table=_storage, Settings={"filter":_filter})
        data_relation = self.__data_relation(data)
        if not data_relation is None:
            operations.records_add(Type="insert", Table=_storage, Settings={"values":data_relation})
        result = self.__infobase.execute_operations(operations, True)
        if not result.success:
            raise Exception(result.result)

    def __data_relation(self, data:list):
        if not data:
            return None
        attributes = {attribute.store_name:attribute.store_type for attribute in data[0]._owner.attributes.values()}
        data_relation = InfoMasterBasic.Relation(attributes)
        for data_record in data:
            data_relation.records_add(data_record.values(by_store_names=True, encoded=True))
        return data_relation

    def __storage_name(self, storage:str, direct_storage_name:bool = False, remove:bool = False) -> str:
        if direct_storage_name:
            return storage
        elif remove:
            return self.__remove_name_prefix(storage)
        else:
            return self.__add_name_prefix(storage)

    def __dict_to_filter(self, filter:dict = None, direct_names:bool = False):
        if filter is None:
            return None
        _filter = InfoMasterBasic.OperationFilter()

        for field, value in filter.items():
            if direct_names:
                _attribute = field
            else:
                _attribute = self.__add_name_prefix(field)
            _filter.records_add(Attribute=_attribute, Compare="=", Value=value, Operation="AND")
        return _filter

    def __add_subobjects(self, object_data, subobject_class, storage_data, object_keys, adding_method):
        for subobject_data in self.__get_subobjects_data(object_data, subobject_class, storage_data,
                                                         object_keys):
            adding_method(is_new=False, **subobject_data)

    def __creating_arguments(self, data_record:InfoMasterBasic._Record, md_class, exclude:list = None) -> dict:
        return {self.__remove_name_prefix(name):md_class.decode_value(self.__remove_name_prefix(name), value)
                for name, value in data_record.values.items()
                if exclude is None or not self.__remove_name_prefix(name) in exclude}

    def __get_subobjects_data(self, object_data:InfoMasterBasic._Record, subobject_class, storage_data:dict,
                              objects_keys:dict) -> list:
        subobjects_data = storage_data[subobject_class].Result
        if subobjects_data:
            object_id = getattr(object_data, self.__add_name_prefix(objects_keys[MDObject]))
            parent_id_field = self.__remove_name_prefix(objects_keys[subobject_class])
            return [self.__creating_arguments(data_record, subobject_class, [parent_id_field])
                    for data_record in subobjects_data.find_value(object_id, [self.__add_name_prefix(parent_id_field)])]
        else:
            return []

    def __read_metadata_from_storage(self, storage_description:dict) -> dict:
        operations = InfoMasterBasic.OperationSet()
        result_dict = {}
        for storage, cls in storage_description.items():
            table = self.__add_name_prefix(storage)
            result_dict[cls] = operations.records_add(Type="select", Table=table)
        result = self.__infobase.execute_operations(operations, True)
        if not result.success:
            raise Exception(result.result)
        return result_dict

    def __add_object_changes(self, operations:InfoMasterBasic.OperationSet, md_object:MDObject,
                             md_tables_description:_EntitiesTablesDescription):
        self.__add_table_changes(operations, md_object)
        self.__add_md_entity_changes(operations, md_object, md_tables_description)
        if not md_object.deletion_mark:
            self.__add__collection_changes(operations, md_object, "attributes", md_tables_description)
            self.__add__collection_changes(operations, md_object, "indexes", md_tables_description)

    def __add_table_changes(self, operations, md_object:MDObject):
        if md_object.deletion_mark:
            operations.records_add(Type = "drop", Table = md_object.store_name)
        elif md_object.is_new:
            attributes = self.__object_attributes(md_object, "standard_attributes", False)
            attributes.update(self.__object_attributes(md_object, "attributes", False))
            operations.records_add(Type = "create", Table = md_object.store_name,
                                   Settings = {"description":self.__table_description(attributes, add_name_prefix=False)})
            self.__add_indexes_change(operations, md_object, "standard_indexes", True)
        else:
            self._add_attribute_changes(operations, md_object)
        self.__add_indexes_change(operations, md_object, "indexes")

    def _add_attribute_changes(self, operations, md_object:MDObject):
        #there are some repeating cycles on attributes but there are not so mamy attributes to cause
        #significant performance issues and such code is more readable and maintainable
        attributes = self.__object_attributes(md_object, "attributes", True)
        for store_name, _type in attributes.items():
            attribute = md_object.get_attribute("store_name", store_name)
            if attribute.deletion_mark and not attribute.is_new:
                operations.records_add(Type="drop_attribute", Table=md_object.store_name, Settings={"name":store_name})
            elif attribute.is_new:
                description = InfoMasterBasic.RelationAttributeDescription(Name=store_name, Type=self.__store_type(_type),
                                                                           Not_null=True)
                operations.records_add(Type="add_attribute", Table=md_object.store_name, Settings={"description":description})
            elif "value_type" in attribute._changes: #неаккуратненько, потом подумать, как сделать приличнее
                description = InfoMasterBasic.RelationAttributeDescription(Type=self.__store_type(_type))
                operations.records_add(Type="alter_attribute", Table=md_object.store_name,
                                       Settings={"name":store_name, "description":description})

    def __add_indexes_change(self, operations, md_object:MDObject, property_name, new_object=False):
        for index in getattr(md_object, property_name):
            if index.deletion_mark and not index.is_new:
                operations.records_add(Type="drop_index", Table=md_object.store_name,
                                       Settings={"name":index.store_name, "if_exists":True})
            elif (index.is_new or new_object) and not index.deletion_mark:
                operations.records_add(Type="index", Table=md_object.store_name,
                                       Settings={"description":self.__index_description(md_object, index)})

    def __index_description(self, md_object:MDObject, index:MDIndex) -> InfoMasterBasic.RelationIndexDescription:
        index_attributes = [md_object[attribute].store_name for attribute in index.attributes]
        return InfoMasterBasic.RelationIndexDescription(name=index.store_name, attributes=index_attributes,
                                                        cluster=index.cluster)

    def __object_attributes(self, md_object:MDObject, property_name, changed_only:bool) -> dict:
        return {attribute.store_name:attribute.value_type for attribute in getattr(md_object, property_name).elements(changed_only)}

    def __add__collection_changes(self, operations, md_object, collection_name, md_tables_desription):
        for element in getattr(md_object, collection_name).elements(changed_only=True):
            self.__add_md_entity_changes(operations, element, md_tables_desription)

    #adds operations for changing metadata description in metadata tables
    def __add_md_entity_changes(self, operations:InfoMasterBasic.OperationSet, entity,
                                md_tables_description:_EntitiesTablesDescription):
        #ПРИМЕЧАНИЕ - ПОТОМ МОЖНО ОТРЕФАКТОРИТЬ, НО ЭТО НЕ СРОЧНО
        md_tables_descr = md_tables_description[entity]
        if entity.deletion_mark:
            for table, id_field in md_tables_descr.delete_from.items():
                _filter = self.__get_filter(entity, id_field)
                operations.records_add(Type = "delete", Table = self.__add_name_prefix(table),
                                       Settings = {"filter":_filter})
        elif entity.is_new:
            entity_properties = {self.__add_name_prefix(name):_type for name,_type in entity._properties.items()}
            values = InfoMasterBasic.Relation(entity_properties)
            values.records_add(self.__entity_properties_values(entity))
            for table, id_field in md_tables_descr.add_to.items():
                operations.records_add(Type = "insert", Table = self.__add_name_prefix(table),
                                       Settings = {"values":values})
        else:
            values = self.__entity_properties_values(entity, True)
            if values:
                for table, id_field in md_tables_descr.add_to.items():
                    _filter = self.__get_filter(entity, id_field)
                    operations.records_add(Type = "update", Table = self.__add_name_prefix(table),
                                           Settings = {"values":values,"filter":_filter})

    def __get_filter(self, entity, id_field) -> InfoMasterBasic.OperationFilter:
        _filter = InfoMasterBasic.OperationFilter()
        _filter.records_add(Attribute=self.__add_name_prefix(id_field), Value=entity._id, Operation='')
        return _filter

    def __entity_properties_values(self, entity, changed_only:bool = False):
        #здесь должно быть преобразование типов value, производимое базовым преобразователем типа basics.TypeManager
        return {self.__add_name_prefix(_property):value for _property, value in entity.properties_values(changed_only, encoded=True).items()}

    def __table_description(self, attributes:dict, add_name_prefix:bool = True) -> InfoMasterBasic.DBRelationDescription:
        description = InfoMasterBasic.DBRelationDescription()
        for name, _type in attributes.items():
            if add_name_prefix:
                _name = self.__add_name_prefix(name)
            else:
                _name = name
            _store_type = self.__store_type(_type)
            description.records_add(Name = _name, Type = _store_type, Not_null = True)
        return description

    def __add_name_prefix(self, name:str):
        return self._name_prefix + name

    def __remove_name_prefix(self, name:str):
        return name.replace(self._name_prefix, "", 1)

    def __store_type(self, _type:type) -> type:
        if _type in self._store_types:
            return _type
        else:
            return str

#data managers factory
class _DataManagersFactory:
    def __init__(self):
        self.__objects = {}

    def register_object(self, key, builder):
        self.__objects[key] = builder

    def create(self, infobase_name:str) -> DataManager:
        #reads settings right before creating data manager in order to use fresh data
        settings = self.__infobase_settings()
        if not infobase_name in settings:
            raise ValueError(f"There is no infobase {infobase_name} in settings file {_infobase_settings_file}")
        elif not settings[infobase_name].get("active"):
            raise Exception(f"Infobase {infobase_name} is not active. Check the 'active' flag in {_infobase_settings_file}")
        data_manager = settings[infobase_name]["data_manager"]
        builder = self.__objects.get(data_manager)
        if not builder:
           raise Exception(f"Supported data managers are {tuple(self.__objects.keys())}.Manager {data_manager} isn't supported!")
        return builder(infobase_name, settings[infobase_name])

    def __infobase_settings(self):
        with open(_infobase_settings_file, "r") as settings_file:
            settings = json.load(settings_file)
        if not isinstance(settings, dict):
            raise Exception(f"Infobase settings file {_infobase_settings_file} is damaged!")
        return settings


class _InfobaseManagersFactory:
    def __init__(self):
        self.__objects = {}

    def register_object(self, key, builder):
        self.__objects[key] = builder

    def create(self, data_manager:str):
        builder = self.__objects.get(data_manager)
        if not builder:
            raise ValueError(f"Supported data managers are {tuple(self.__objects.keys())}.Manager {data_manager} isn't supported!")
        return builder()

#init
infobase_manager_factory = _InfobaseManagersFactory()
infobase_manager_factory.register_object("InfoMaster", InfoMasterInfobaseManager)

data_managers_factory = _DataManagersFactory()
data_managers_factory.register_object("InfoMaster", InfoMasterDataManager)


