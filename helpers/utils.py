"""Utility functions for manipulating the station objects."""
from math import (
    radians as deg2rad,
    sin,
    cos,
    atan2,
    sqrt
)


def add_alias(data_map):
    """Add an alias station_id to every Station object."""
    # TODO TdR 28-4-2016 Closeby stations should have closeby aliases.
    alias_id = 1
    for station_id in data_map:
        station = data_map[station_id]
        station.alias = alias_id
        alias_id += 1


def get_station_coordinates(data_map):
    """Return station coordinates for every station in data_map."""
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


def _distance(point1, point2):
    r"""Compute the haversine distance between two (lat,lon) points.

    This method assumes the earth is a sphere with a constant radius.
    The error is 0.55\%, which is good enough for most uses.
    Returns distance in whole meters.
    """
    (lat1, lon1) = point1
    (lat2, lon2) = point2
    R = 6371000  # Radius of the earth in meters

    # Convert degrees to radians
    phi_1 = deg2rad(lat1)
    phi_2 = deg2rad(lat2)
    delta_phi = deg2rad(lat2 - lat1)
    delta_lambda = deg2rad(lon2 - lon1)

    # Haversine
    a = sin(delta_phi / 2) * sin(delta_phi / 2) + \
        cos(phi_1) * cos(phi_2) * \
        sin(delta_lambda / 2) * sin(delta_lambda / 2)

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    d = R * c
    return int(round(d))


def select_near(data_map, latitude, longitude, radius=5000):
    """Filter data_map by selecting a region of interest.

    parameters
    ----------
    data_map: dict or list, mapping of station id to Station objects.
    latitude: float, latitude of interest.
    longitude: float, longitude of interest.
    radius: float (optional), maximum search radius in meters.
    """

    if isinstance(data_map, dict):
        filtered_map = {}
        for station_id in data_map:
            station = data_map[station_id]
            station_dist = _distance(
                (station.latitude, station.longitude),
                (latitude, longitude)
            )
            if station_dist <= radius:
                filtered_map[station_id] = station
        return filtered_map
    else:
        filtered_map = []
        for station in data_map:
            station_dist = _distance(
                (station.latitude, station.longitude),
                (latitude, longitude)
            )
            if station_dist <= radius:
                filtered_map.append(station)
        return filtered_map
