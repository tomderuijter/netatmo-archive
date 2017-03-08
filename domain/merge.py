from domain.base import Station


def merge_documents(documents):
    station_dict = {}

    for document in documents:
        station_id = document.station_id['station_id']

        if station_id not in station_dict:
            station_dict[station_id] = document
        else:
            stack = station_dict[station_id]
            station_dict[station_id] = merge_stations(stack, document)
    return list(station_dict.values())


def clean_id(station):
    if isinstance(station.station_id, dict):
        station.station_id = station.station_id['station_id']


# TODO Move to base class
def merge_stations(station_1, station_2):
    assert isinstance(station_1, Station)
    assert isinstance(station_2, Station)

    clean_id(station_1)
    clean_id(station_2)
    assert station_1.station_id == station_2.station_id

    merge_location(station_1, station_2)
    merge_thermo_module(station_1, station_2)
    merge_hydro_module(station_1, station_2)
    return station_1


def merge_location(station_1, station_2):
    # TODO tomdr 08/03/17: store if changed.
    pass


def merge_thermo_module(station_1, station_2):
    merge_dict_of_lists(station_1.thermo_module, station_2.thermo_module)


def merge_hydro_module(station_1, station_2):
    merge_dict_of_lists(station_1.hydro_module, station_2.hydro_module)


def merge_dict_of_lists(d1, d2):
    assert len(d1) == len(d2)
    attrs = list(d1.keys())
    for attr in attrs:
        assert isinstance(d1[attr], list)
        assert isinstance(d2[attr], list)
        d1[attr] += d2[attr]
