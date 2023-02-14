#main module of the service
from information_service.basics import Numerator, IDGenerator, ObjectLink
from information_service.md_objects import MDEntityCollection, MDObject, MDAttribute, MDIndex, MDObjectReference
from information_service.md_objects import _EntityTablesDescription, _EntitiesTablesDescription, MDObjectsTypes
from information_service.data_managers import data_managers_factory

class _MetadataMeta(type):

    _instances = {}

    def __call__(cls, infobase, **kwargs):
        if infobase not in cls._instances:
            instance = super().__call__(infobase, **kwargs)
            cls._instances[infobase] = instance
        return cls._instances[infobase]

class Metadata(metaclass=_MetadataMeta):
    _available_md_types = {MDObjectsTypes.reference:MDObjectReference}
    _metadata_objects = {"_md_objects":MDObject, "_md_attributes":MDAttribute, "_md_indexes":MDIndex}
    _metadata_object_keys = {MDObject:"_id", MDAttribute:"_object_id", MDIndex:"_object_id"}
    _metadata_storages = {_object:storage for storage, _object in _metadata_objects.items()}
    _link_md_types = [MDObjectsTypes.reference]

    @classmethod
    def _metadata_description(cls):
        return {name:obj._properties for name, obj in cls._metadata_objects.items()}

    @staticmethod
    def _tables_description() -> _EntitiesTablesDescription:
        tables_description = _EntitiesTablesDescription()
        tables_description[MDObject] = _EntityTablesDescription(add_to={"_md_objects":"_id"},
                                                             delete_from={"_md_objects":"_id",
                                                                          "_md_attributes":"odject_id",
                                                                          "_md_indexes":"odject_id"}
                                                             )
        tables_description[MDAttribute] = _EntityTablesDescription(add_to={"_md_attributes":"_id"},
                                                                delete_from={"_md_attributes":"_id"})
        tables_description[MDIndex] = _EntityTablesDescription(add_to={"_md_indexes":"_id"},
                                                              delete_from={"_md_indexes":"_id"})
        return tables_description

    def __init__(self, infobase:str):
        self.__objects = MDEntityCollection(value_class=MDObject, owner=self)
        self.__changed = False
        self.__data_manager = data_managers_factory.create(infobase_name=infobase)
        self.__infobase = infobase

    @property
    def changed(self):
        return self.__changed

    @staticmethod
    def md_object_types():
        return MDObjectsTypes

    def create_metadata_storage(self):
        #ПРИМЕЧАНИЕ - ПОТОМ МОЖНО ПРИКРУТИТЬ К ТАБЛИЦАМ МЕТАДАННЫХ ИНДЕКСЫ
        description = self._metadata_description()
        self.__data_manager.create_metadata_storage(description)

    def read_metadata(self):
        self.__objects.clear()
        self.__data_manager.read_metadata(self)

    def save_changes(self):
        if self.__changed:
            self.__data_manager.save_changes(objects=self.objects(changed_only=True),
                                             md_tables_description=self._tables_description())
        #ПРИМЕЧАНИЕ - ПОСЛЕ ПРОЦЕДУРЫ НУЖНО ВСЕ ЭЛЕМЕНТЫ ПОМЕТИТЬ КАК ИЗМЕНЕННЫЕ. ПОДУМАТЬ, КАК ЭТО ПООПТИМАЛЬНЕЕ СДЕЛАТЬ
        #ПРИМЕЧАНИЕ - ПОТОМ РАЗОБРАТЬСЯ С АЛЛЕРГИЕЙ Postgre на верхний регистр (принудительно указывать md_types только в нижнем
        # - ну так себе мера (не тот уровень абстракций). ну или хотя бы проверку для новых типов прикрутить)
        #ПРИМЕЧАНИЕ - НЕ ЗАБЫТЬ ЗАПРЕТИТЬ ИЗМЕНЕНИЕ ТИПА СУЩЕСТВУЮЩИХ АТРИБУТОВ ДЛЯ НЕ ПУСТЫХ ТАБЛИЦ

    def add_object(self, md_type:str, name:str, _id:str = None, store_name:str = None, is_new:bool = True) -> MDObject:
        if not md_type in self._available_md_types:
            raise ValueError(f"Metadata objects {md_type} in not in available types {self._available_md_types}")
        if not _id:
            _id = IDGenerator.generate()
        if not store_name:
            prefix = f"_{md_type}"
            store_name = Numerator.next_numerated_value(objects=self.__objects.elements(), property_name="store_name", prefix=prefix)
        return self.__objects.add_element(_id=_id, store_name=store_name, is_new=is_new, name=name, value_class=self._available_md_types[md_type])

    def drop_object(self, obj:MDObject):
        self.__objects.set_deletion_mark(obj, True)

    def get_object(self, md_type:str, name:str) -> MDObject:
        for _object in self.__objects.elements():
            if _object.md_type == md_type and _object.name == name:
                return _object
        return None

    def get_object_link(self, objects) -> ObjectLink:
        if not isinstance(objects, MDObject) and not isinstance(objects, list):
            raise TypeError("Expected MDObject or list of MDObject!")
        if isinstance(objects, MDObject):
            _objects = [objects]
        else:
            _objects = objects
        objects_links = [self.__object_link(_object) for _object in _objects]
        return ObjectLink(objects_links)

    def objects(self, changed_only:bool = False):
        return self.__objects.elements(changed_only)

    def object_settings(self, object_id:str = None, md_type:str = None, name:str = None):
        #ПРИМЕЧАНИЕ - ПОТОМ МОЖНО ВОТКНУТЬ ПРОВЕРКИ НА ОТСУТСТВИЕ РЕЗУЛЬТАТА ЛИБО ТАКОГО ОБЪЕКТА В МЕТАДАННЫХ
        if not object_id is None:
            _filter = {"_id":object_id}
        elif md_type is None or name is None:
            raise ValueError("object_id or md_type and name shouldn't be None")
        else:
            _filter = {"md_type":md_type, "name":name}
        return self.__data_manager.read_data(storage=self._metadata_storages[MDObject],filter=_filter)[0]

    def _notify_child_changes(self, child_objects, changes_exist:bool):
        self.__changed = changes_exist

    def __object_link(self, md_object:MDObject):
        if not isinstance(md_object, MDObject):
            raise TypeError(f"type {type(md_object)} is not MDObject!")
        if not md_object in self.__objects:
            raise ValueError(f"Object {object.name} doesn't belong {self.__infobase} metadata. "
                             f"Perhaps you've forgotten read_metadata()")
        if not md_object.md_type in self._link_md_types:
            raise TypeError(f"Metadata type {md_object.md_type} doesn't belong link types {self._link_md_types}")
        return md_object._id