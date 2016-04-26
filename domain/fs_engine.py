"""Module for accessing NetAtmo files using the file system."""
import os
import json
import gzip
import pandas as pd
from numpy import nan
from datetime import datetime, timedelta


# User modules
from .base import (
    DataRequest,
    DataResponse,
    Station,
)


class FileSystemEngine(object):
    """Translate the query to response using the file system."""

    # TODO Extract a common class engine

    def __init__(self, dir_path):
        """Constructor."""
        self.directory = dir_path

    def ls(self):
        """List json file objects in engine directory."""
        return_list = [
            f for f in os.listdir(self.directory) if f.endswith(".json.gz")
        ]
        return_list.sort()
        return return_list

    def query(self, request):
        """Query the file system."""
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
        print("Loading %d files in total." % (len(request_file_names)))
        for (count, file_name) in enumerate(request_file_names):
            print("File %d: %s" % (count + 1, file_name))
            # File does not exist.
            if file_name not in existing_files:
                print("File does not exist\n")
                continue

            # Open file and parse json
            json_data = load_file(self.directory, file_name)
            if json_data is None:
                raise RuntimeError()

            # Extract and add data
            new_stations, station_contributions, out_of_region = \
                parse_stations(json_data, data_map, request.region)

            # Ingestion logging.
            print("Total number of stations:           %d" %
                  (len(data_map)))
            print("Points in file:                     %d" %
                  (len(json_data)))
            print("Points out of region:               %d" %
                  (out_of_region))
            print("Points ignored:                     %d" %
                  (len(json_data) - out_of_region - station_contributions))
            print("New stations:                       %d" %
                  (new_stations))
            print("Points added to dataset:            %d" %
                  (station_contributions))
            print()
        # Convert dictionary to pandas dataframe
        # print("Resampling and interpolating station data..")
        # resample_and_interpolate(data_map)

        # Create response object
        response = DataResponse()
        response.data_map = data_map
        return response


def save_file(obj, file_path, file_name):
    """Compress and store a json object to disk."""
    with gzip.open(file_path + file_name, "wb") as fp:
        fp.write(json.dumps(obj).encode('utf-8'))


def load_file(file_path, file_name):
    """Load a compressed json file from disk."""
    with gzip.open(file_path + file_name, "rb") as fp:
        return json.loads(fp.read().decode('utf-8'))


def parse_stations(station_list, data_map, region=None):
    """Given contents of a single data file, update the given data objects.

    parameters
    ----------
    station_list: list, list of all station ids included in data_map
    data_map: dict, mapping of station ids to Station objects
    """

    new_stations = 0
    station_contributions = 0
    out_of_region = 0

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
            out_of_region += 1
            continue

        # Add station to map
        if station_id not in data_map:
            new_stations += 1
            data_map[station_id] = Station(station_id, lat, lon)

        try:
            success = parse_station_data(point['data'], data_map[station_id])
            if success:
                station_contributions += 1
        except IOError:
            continue
    return new_stations, station_contributions, out_of_region


def inside_box(lat, lon, tl_lat, tl_lon, br_lat, br_lon):
    """Whether a coordinate is inside a bounding box.

    Given query point (lat, lon) considers whether point is indide the box
    defined by top left point (tl_lat, tl_lon) and bottom right point
    (br_lat, br_lon).
    """
    return lat >= br_lat and lat <= tl_lat and lon >= tl_lon and lon <= br_lon


def parse_station_data(station_data, station):
    """Parse data from a single json record.

    parameters
    ----------
    station_data: dict, contains atmospherical values
    station: Station object
    """
    # TODO Not finished: this does not support other netatmo modules
    if 'time_utc' not in station_data:
        raise IOError("no base module timestamp in data")

    valid_datetime = datetime.utcfromtimestamp(station_data['time_utc'])

    # Simple duplicate detection
    if station.thermo_module is not None and \
       station.thermo_module['valid_datetime'][-1] == valid_datetime:
        raise IOError("duplicate measurement")

    if station.thermo_module is None:
        station.thermo_module = {
            'valid_datetime': [],
            'temperature': [],
            'humidity': [],
            'pressure': []
        }

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
    """Convert a datetime timestamp to the necessary file name.

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


# TODO Really really slow. Works well for large amounts per station.
def resample_and_interpolate(data_map, resolution=10):
    """Resample and interpolate a dictionary to pandas dataframe."""
    for count, station_id in enumerate(data_map):
        if count % 1000 == 0:
            print("%d / %d stations processed.." % (count, len(data_map)))

        station = data_map[station_id]

        if station.thermo_module is not None:
            df = pd.DataFrame(station.thermo_module)
            df.set_index('valid_datetime', drop=True, inplace=True)
            df = df.resample(str(resolution) + 'T').interpolate()
        else:
            df = pd.DataFrame()
        station.thermo_module = df
