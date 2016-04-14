"""Utility functions for manipulating the station objects"""


def get_station_coordinates(data_map):
    """Returns station coordinates for every station in data_map"""
    station_coords = []
    for station_id in data_map:
        station = data_map[station_id]
        station_coords.append(
            [station.latitude, station.longitude, station.station_id])
    return station_coords


def add_station_elevations(data_map, station_ids, elevations):
    for count, station_id in enumerate(station_ids):
        if station_id not in data_map:
            continue
        data_map[station_id].elevation = elevations[count]
