"""Module for communicating with MongoDB."""
import pymongo
from domain.base import Station


class MongoDBConnector(object):
    """Connector class for reading and writing NetAtmo data."""

    def __init__(self):
        # Write concern describes the level of acknowledgement
        # requested from MongoDB for write operations. Turning it
        # off may result in performance increase for write operations.
        # This comes at the cost of error reporting.
        write_concern = 1
        self._client = pymongo.MongoClient(w=write_concern)
        self.db = self._client.netatmo

    def close(self):
        self._client.close()

    def upsert_stations(self, station_dict):
        bulk = self.db.stations.initialize_unordered_bulk_op()
        for station_id in station_dict:
            station = station_dict[station_id]
            query = {'_id': station_id}
            update = _construct_station_upsert_query(station)
            bulk.find(query).upsert().update(update)
            # self.db.stations.update(query, update, True)
        bulk.execute()

    def insert_station(self, station):
        assert isinstance(station, Station)
        station_dict = station.__dict__
        _add_primary_key(station_dict)
        self.db.stations.insert_one(station_dict)

    def get_station(self, station_id):
        return self.db.stations.find_one({'station_id': station_id})

    def get_all_station_locations(self):
        return self.db.stations.find(
            {},
            {'_id': 0, 'station_id': 1, 'latitude': 1, 'longitude': 1, 'elevation': 1}
        )


def _add_primary_key(station_dict):
    station_dict['_id'] = station_dict['station_id']


def _construct_station_upsert_query(station):
    update = {
        '$setOnInsert': {
            '_id': station.station_id,
            'elevation': station.elevation,
            'latitude': station.latitude,
            'longitude': station.longitude
        },
        '$push': {}
    }

    if station.hydro_module is not None:
        update['$push']['hydro_module.time_day_rain'] = {'$each': station.hydro_module['time_day_rain']}
        update['$push']['hydro_module.time_hour_rain'] = {'$each': station.hydro_module['time_hour_rain']}
        update['$push']['hydro_module.daily_rain_sum'] = {'$each': station.hydro_module['daily_rain_sum']}
        update['$push']['hydro_module.hourly_rain_sum'] = {'$each': station.hydro_module['hourly_rain_sum']}
    else:
        update['$setOnInsert']['hydro_module'] = None

    if station.thermo_module is not None:
        update['$push']['thermo_module.humidity'] = {'$each': station.thermo_module['humidity']}
        update['$push']['thermo_module.pressure'] = {'$each': station.thermo_module['pressure']}
        update['$push']['thermo_module.temperature'] = {'$each': station.thermo_module['temperature']}
        update['$push']['thermo_module.valid_datetime'] = {'$each': station.thermo_module['valid_datetime']}
    else:
        update['$setOnInsert']['thermo_module'] = None

    if update['$push'] == {}:
        del update['$push']

    return update
