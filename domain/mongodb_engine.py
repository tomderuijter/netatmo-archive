"""Module for communicating with MongoDB."""
import pymongo
from domain.base import Station


class MongoDBConnector():
    """Connector class for reading and writing NetAtmo data."""

    def __init__(self):
        # TODO Throw error if module fails
        self.db = pymongo.MongoClient().netatmo

    def write_station(self, station):
        assert isinstance(station, Station)
        self.db.stations.insert_one(station.__dict__)

    def get_station(self, station_id):
        self.db.stations.find_one({'station_id': station_id})

    def update_station(self, station):
        # TODO
        pass

    def delete_station(self, station):
        # TODO
        pass

    def find_nearby_stations(self, point, radius):
        latitude, longitude = point
        # TODO
        pass

    def find_stations_in_box(top_left_point, bottom_right_point):
        left_latitude, top_longitude = top_left_point
        right_latitude, bottom_longitude = bottom_right_point
        # TODO
        pass
