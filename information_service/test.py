from information_service.basics import TypeManager, ObjectLink
from information_service.metadata import Metadata
from information_service.data_objects import DataManager

_metadata = Metadata("adas")
_data_manager = DataManager("adas")
good_object = _data_manager.reference_manager("goods").find_object({'_id':'79e4dba9-316e-4162-a065-99d6d1a58648'})
good_object._name = "good2"
good_object.write()
_prices_object = _data_manager.reference_manager("prices").get_list()[0].get_object()
print(_prices_object.good.values)
