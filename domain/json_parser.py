from datetime import datetime
from numpy import nan
import logging

# User modules
from domain.base import Station


def parse_stations(station_list, data_map, region=None):
    """Given contents of a single data file, update the given data objects.

    parameters
    ----------
    station_list: list, list of all station ids included in data_map
    data_map: dict, mapping of station ids to Station objects
    """
    # new_stations = 0
    # station_contributions = 0
    # out_of_region = 0
    statistics = {}
    statistics['new_stations'] = 0
    statistics['station_thermo_contributions'] = 0
    statistics['station_hydro_contributions'] = 0
    statistics['stations_out_of_region'] = 0
    statistics['station_count'] = 0
    statistics['stations_in_file'] = len(station_list)

    for point in station_list:
        # Data sanitization
        if 'location' not in point:
            continue
        if '_id' not in point:
            continue
        if 'data' not in point:
            continue

        # Extract data
        station_id = point['_id']
        lon, lat = point['location']

        #   See if station is in requested region
        if region is not None and not _is_inside_box(lat, lon, *region):
            statistics['stations_out_of_region'] += 1
            continue

        # Add station to map
        if station_id not in data_map:
            statistics['new_stations'] += 1
            data_map[station_id] = Station(station_id, lat, lon)

        thermo_success = \
            parse_station_thermo_data(point['data'], data_map[station_id])
        if thermo_success:
            statistics['station_thermo_contributions'] += 1

        hydro_success = \
            parse_station_hydro_data(point['data'], data_map[station_id])
        if hydro_success:
            statistics['station_hydro_contributions'] += 1

    statistics['station_count'] = len(data_map)
    return statistics


def parse_station_hydro_data(station_data, station):
    """Parse precipitation data from a single json record.

    parameters
    ----------
    station_data: dict, contains atmospherical values
    station: Station object
    """
    if ('time_day_rain' not in station_data) or \
       ('time_hour_rain' not in station_data):
        return False

    time_day_rain = datetime.utcfromtimestamp(station_data['time_day_rain'])
    time_hour_rain = datetime.utcfromtimestamp(station_data['time_hour_rain'])

    # TODO 30-06-2016 TdR: Simple duplicate detection of records.

    station.hydro_module['time_day_rain'].append(time_day_rain)
    station.hydro_module['time_hour_rain'].append(time_hour_rain)
    _add_value(station_data, 'Rain', station.hydro_module,
               'daily_rain_sum')
    _add_value(station_data, 'sum_rain_1', station.hydro_module,
               'hourly_rain_sum')
    return True


def parse_station_thermo_data(station_data, station):
    """Parse data from a single json record.

    parameters
    ----------
    station_data: dict, contains atmospherical values
    station: Station object
    """
    if 'time_utc' not in station_data:
        return False

    valid_datetime = datetime.utcfromtimestamp(station_data['time_utc'])

    # Simple duplicate detection
    if station.thermo_module['valid_datetime'] != [] and \
       station.thermo_module['valid_datetime'][-1] == valid_datetime:
        return False

    station.thermo_module['valid_datetime'].append(valid_datetime)
    _add_value(station_data, 'Temperature', station.thermo_module,
               'temperature')
    _add_value(station_data, 'Humidity', station.thermo_module, 'humidity')
    _add_value(station_data, 'Pressure', station.thermo_module, 'pressure')
    return True


def _add_value(input_dict, input_name, output_dict, output_name):
    value = nan
    if input_name in input_dict:
        value = input_dict[input_name]
    output_dict[output_name].append(value)


def _is_inside_box(lat, lon, tl_lat, tl_lon, br_lat, br_lon):
    """Whether a coordinate is inside a bounding box. """
    return (br_lat <= lat <= tl_lat) and (tl_lon <= lon <= br_lon)


def log_parse_stats(parse_stats):
    # Ingestion logging.
    logging.debug("Total number of stations:           %d" %
          (parse_stats['station_count']))
    logging.debug("Points in file:                     %d" %
          (parse_stats['stations_in_file']))
    logging.debug("Points out of region:               %d" %
          (parse_stats['stations_out_of_region']))
    logging.debug("New stations:                       %d" %
          (parse_stats['new_stations']))
    logging.debug("Thermo measurements added:          %d" %
          (parse_stats['station_thermo_contributions']))
    logging.debug("Hydro measurements added:           %d" %
          (parse_stats['station_hydro_contributions']))
