"""Module for communicating with MongoDB."""
import logging

import pymongo
import pymongo.son_manipulator
from bson.binary import Binary

from domain.base import Station


class MongoDBConnector(object):
    """Connector class for reading and writing NetAtmo data."""

    def __init__(self):
        # Write concern describes the level of acknowledgement
        # requested from MongoDB for write operations. Turning it
        # off may result in performance increase for write operations.
        # This comes at the cost of error reporting.
        write_concern = 1
        # TODO TdR 08/12/16: configure database.
        self._client = pymongo.MongoClient(w=write_concern)
        self.db = self._client.netatmo  # Database name
        # TODO TdR 08/07/16: Objects are not yet pushed in as stations.
        self.db.add_son_manipulator(BinaryTransformer())

    def close(self):
        self._client.close()

    def upsert_stations(self, station_dict):
        """Update station records or insert them otherwise."""
        skipped = 0
        bulk = self.db.stations.initialize_unordered_bulk_op()
        for station_id in station_dict:
            try:
                station = station_dict[station_id]

                query = {'_id': _get_primary_key(station)}
                update = _construct_station_upsert_query(station)
                bulk.find(query).upsert().update(update)
                # self.db.stations.update(query, update, True)
            except RuntimeError:
                skipped += 1
                continue
        bulk.execute()
        logging.info("%d records were skipped due to missing data." % skipped)


def _get_primary_key(station):
    date = _get_current_date(station)
    return {'station_id': station.station_id, "date": date}


def _get_current_date(station):

    if station.thermo_module is not None and \
                    len(station.thermo_module['valid_datetime']) > 0:

        return date_to_str(station.thermo_module['valid_datetime'][0])
    elif station.thermo_module is not None and \
            len(station.hydro_module['time_hour_rain']) > 0:
        return date_to_str(station.hydro_module['time_hour_rain'][0])
    else:
        raise RuntimeError("No data in record.")


def date_to_str(datetime):
    date_str = "%Y%m%d"
    return datetime.date().strftime(date_str)


def _construct_station_upsert_query(station):
    update = {
        '$setOnInsert': {
            '_id': _get_primary_key(station),
            'elevation': station.elevation,
            'latitude': station.latitude,
            'longitude': station.longitude
        },
        '$push': {}
    }

    if station.hydro_module is not None:
        update['$push']['hydro_module.time_day_rain'] = {
            '$each': station.hydro_module['time_day_rain']}
        update['$push']['hydro_module.time_hour_rain'] = {
            '$each': station.hydro_module['time_hour_rain']}
        update['$push']['hydro_module.daily_rain_sum'] = {
            '$each': station.hydro_module['daily_rain_sum']}
        update['$push']['hydro_module.hourly_rain_sum'] = {
            '$each': station.hydro_module['hourly_rain_sum']}
    else:
        update['$setOnInsert']['hydro_module'] = None

    if station.thermo_module is not None:
        update['$push']['thermo_module.humidity'] = {
            '$each': station.thermo_module['humidity']}
        update['$push']['thermo_module.pressure'] = {
            '$each': station.thermo_module['pressure']}
        update['$push']['thermo_module.temperature'] = {
            '$each': station.thermo_module['temperature']}
        update['$push']['thermo_module.valid_datetime'] = {
            '$each': station.thermo_module['valid_datetime']}
    else:
        update['$setOnInsert']['thermo_module'] = None

    if update['$push'] == {}:
        del update['$push']

    return update


class BinaryTransformer(pymongo.son_manipulator.SONManipulator):

    def transform_incoming(self, son, collection):
        for (key, value) in son.items():
            if isinstance(value, Station):
                son[key] = value.to_binary()
            elif isinstance(value, dict):
                son[key] = self.transform_incoming(value, collection)
        return son

    def transform_outgoing(self, son, collection):
        for (key, value) in son.items():
            if isinstance(value, Binary) and \
                            value.subtype == Station.binary_subtype:
                son[key] = Station.from_binary(value)
            elif isinstance(value, dict):
                son[key] = self.transform_outgoing(value, collection)
        return son
