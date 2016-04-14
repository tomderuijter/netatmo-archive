import os
import json
import gzip
from datetime import datetime, timedelta


# User modules
from .base import (
    DataRequest,
    DataResponse,
    Station,
    ObservationData
)


class FileSystemEngine(object):
    """Translates query to response using the file system"""
    # TODO Extract a common class engine

    def __init__(self, dir_path):
        self.directory = dir_path

    def ls(self):
        """List json file objects in engine directory"""
        return_list = [
            f for f in os.listdir(self.directory) if f.endswith(".json.gz")
        ]
        return_list.sort()
        return return_list

    def query(self, request):
        assert isinstance(request, DataRequest)

        # Initialize data objects
        data_map = {}

        # Select all possible datetimes of interest
        request_datetime_range = datetime_range(
            request.start_datetime,
            request.end_datetime,
            request.time_resolution
        )
        # Translate request datetime into files
        request_file_names = [
            datetime_to_file_name(ts) for ts in request_datetime_range
        ]
        # List existing files
        existing_files = self.ls()

        # Load request files
        print("Loading %d files.." % (len(request_file_names)))
        for (count, file_name) in enumerate(request_file_names):
            print("File %d.." % (count + 1))
            # File does not exist.
            if file_name not in existing_files:
                print("File %s does not exist" % file_name)
                continue

            # Open file and parse json
            json_data = load_file(self.directory, file_name)
            if json_data is None:
                raise RuntimeError()

            # Extract and add data
            parse_stations(json_data, data_map, request.region)

        # Create response object
        response = DataResponse()
        response.data_map = data_map
        return response


def save_file(obj, file_path, file_name):
    """Compress and store a json object to disk"""
    with gzip.open(file_path + file_name, "wb") as fp:
        fp.write(json.dumps(obj).encode('utf-8'))


def load_file(file_path, file_name):
    """Load a compressed json file from disk"""
    with gzip.open(file_path + file_name, "rb") as fp:
        return json.loads(fp.read().decode('utf-8'))


def parse_stations(station_list, data_map, region=None):
    """Given contents of a single data file, update the given data objects.

    parameters
    ----------
    station_list: list, list of all station ids included in data_map
    data_map: dict, mapping of station ids to Station objects
    """

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
        if region is not None and not inside_box(lat, lon, *region):
            continue

        # Add station to map
        if station_id not in data_map:
            data_map[station_id] = Station(station_id, lat, lon)

        try:
            parse_station_data(point['data'], data_map[station_id])
        except IOError:
            continue


def inside_box(lat, lon, tl_lat, tl_lon, br_lat, br_lon):
    """Given query point (lat, lon) considers whether point is indide the box
    defined by top left point (tl_lat, tl_lon) and bottom right point
    (br_lat, br_lon).
    """
    return lat >= br_lat and lat <= tl_lat and lon >= tl_lon and lon <= br_lon


def parse_station_data(station_data, station):
    """Parse data from a single json record

    parameters
    ----------
    station_data: dict, contains atmospherical values
    station: Station object"""

    # TODO Not finished: this does not support other netatmo modules
    if 'time_utc' not in station_data:
        raise IOError("no timestamp in data")

    valid_datetime = datetime.utcfromtimestamp(station_data['time_utc'])

    # Simple duplicate detection
    if len(station.forecast_data) > 0 and \
       station.forecast_data[-1].valid_datetime >= valid_datetime:
        raise IOError("duplicate measurement or earlier measurement")

    observation = ObservationData()
    observation.valid_datetime = valid_datetime
    if 'Temperature' in station_data:
        observation.temperature = station_data['Temperature']
    if 'Humidity' in station_data:
        observation.humidity = station_data['Humidity']
    if 'Pressure' in station_data:
        observation.pressure = station_data['Pressure']
    station.forecast_data.append(observation)


def datetime_range(start_timestamp, end_timestamp, stepsize):
    """Range function, specified for datetime.datetime classes.

    parameters
    ----------
    start_timestamp: datetime.datetime, in UTC
    end_timestamp: datetime.datetime, in UTC
    stepsize: int, stepsize in minutes
    """
    r = []
    ts = start_timestamp
    while ts <= end_timestamp:
        r.append(ts)
        ts += timedelta(minutes=stepsize)
    return r


def datetime_to_file_name(timestamp):
    """Converts a datetime timestamp to the necessary file name.
    parameters
    ----------
    timestamp: datetime.datetime object
    """
    # Example datetime object.
    # datetime(year, month, day, hour, min, tzinfo=timezone.utc)
    assert isinstance(timestamp, datetime)

    return "netatmo_%s%s%s_%s%s.json.gz" % (
        str(timestamp.year).zfill(4),
        str(timestamp.month).zfill(2),
        str(timestamp.day).zfill(2),
        str(timestamp.hour).zfill(2),
        str(timestamp.minute).zfill(2)
    )
