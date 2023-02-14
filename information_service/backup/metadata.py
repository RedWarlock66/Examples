#module for core metadata operations
import information_service.metadata_objects as metadata_objects
from information_service.data_managers import data_managers_factory
from information_service.data_objects import DataRelation

#core metadata class
class Metadata:

    _md_types = {"ObjectRelation":metadata_objects.MetadataObjectRelation}
    _md_relations_table = "_md_relations"
    _md_attributes_table = "_md_attributes"

    def __init__(self, infobase:str):
        self.__infobase = infobase
        self.__data_manager = data_managers_factory.create(infobase)
        self.__objects = {} #key - id, value - metadata_objects._ObjectDescription
        self.__added_objects = []
        self.__dropped_objects = []

    def create_metadata(self):
        self.__data_manager.create_metadata(self.__metadata_description())

    def add_object(self, md_type:str):
        if not md_type in self._md_types:
            raise ValueError(f"There is no {md_type} in available md_types {list(self._md_types)}")
        object_id = metadata_objects._IDGenerator.generate()
        object_name = f"{md_type}{len(self.__objects)+1}"
        md_object = metadata_objects._ObjectDescription(md_type, self._md_types[md_type](name = object_name))
        self.__objects[object_id] = md_object
        self.__added_objects.append(object_id)

    def drop_object(self, object_id:str):
        #ПРИМЕЧАНИЕ - ТУТ ДОЛЖНА БЫТЬ ПРОВЕРКА НА НАЛИЧИЕ ССЫЛОК НА ОБЪЕКТ
        if not object_id in self.__objects:
            raise ValueError(f"There is no object id {object_id}")
        self.__objects.pop(object_id)
        if object_id in self.__added_objects:
            self.__added_objects.remove(object_id)
        else:
            self.__dropped_objects.append(object_id)

    def get_object(self, object_id:str):
        if not object_id in self.__objects:
            raise ValueError(f"There is no object id {object_id}")
        return self.__objects[object_id]

    def save_changes(self):
        changes = self.__get_changes()

    #service
    def _check_name(self, md_type, name):
        for object in self.__objects.values():
            if object.md_type == md_type and object.object.name == name:
                raise ValueError(f"There is object {md_type} name {name}! Choose another name")

    def __get_changes(self):
        added_objects = {key: value for key, value in self.__objects.items() if key in self.__added_objects}
        altered_objects = {key: value for key, value in self.__objects.items() if value.changed}

    def __metadata_description(self):
        #ПРИМЕЧАНИЕ - ПОТОМ ПОДУМАТЬ, НУЖНА ЛИ ВОЗМОЖНОСТЬ РАСШИРЕНИЯ СВОЙСТВ АТТРИБУТОВ И КАК ЕЁ СДЕЛАТЬ ПРИЛИЧНЕЕ
        #(ПОКА ПРИДЕТСЯ ПРАВИТЬ КОД В НЕСКОЛЬКИХ МЕСТАХ)
        #ПРИМЕЧАНИЕ - СНАЧАЛА СДЕЛАТЬ ОСНОВНОЕ, ПОТОМ - РЕФАКТОРИТЬ
        _md_relations = metadata_objects._MetadataDescription(self._md_relations_table, "id")
        _md_relations.add_attribute(name="id", value_type=str, not_null=True)
        _md_relations.add_attribute(name="name", value_type=str, not_null=True)
        _md_relations.add_attribute(name="md_type", value_type=str, not_null=True)
        _md_relations.add_index(attributes=["id"], clustered=True)
        _md_relations.add_index(attributes=["md_type", "id"])

        _md_attributes = metadata_objects._MetadataDescription(self._md_attributes_table, "id")
        _md_attributes.add_attribute(name="id", value_type=str, not_null=True)
        _md_attributes.add_attribute(name="relation_id", value_type=str, not_null=True)
        _md_attributes.add_attribute(name="name", value_type=str, not_null=True)
        _md_attributes.add_attribute(name="value_type", value_type=str, not_null=True)
        _md_attributes.add_attribute(name="not_null", value_type=bool, not_null=True)
        _md_attributes.add_attribute(name="default", value_type=str, not_null=True)
        _md_attributes.add_index(attributes=["id"], clustered=True)
        _md_attributes.add_index(attributes=["relation_id", "id"], clustered=True)

        return [_md_relations, _md_attributes]
