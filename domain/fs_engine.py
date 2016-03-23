import os
import json
from datetime import datetime, timedelta


# User modules
from base import (
    DataRequest,
    DataResponse
)


class FileSystemEngine(object):
    """Translates query to response using the file system"""
    # TODO Extract a common class engine

    def init(self, dir_path):
        self.directory = dir_path

    def ls(self):
        """List json file objects in engine directory"""
        return [
            f for f in os.listdir(self.directory) if f.endswith(".json")
        ]

    def query(self, request):
        assert isinstance(request, DataRequest)

        # Initialize data objects
        station_ids = []
        data_map = {}

        # Select all possible datetimes of interest
        request_datetime_range = datetime_range(
            request.start_date_time,
            request.end_date_time,
            request.time_resolution
        )
        # Translate request datetime into files
        request_file_names = [
            datetime_to_file_name(ts) for ts in request_datetime_range
        ]
        # List existing files
        existing_files = self.ls()

        # Load request files
        for file_name in request_file_names:
            # File does not exist.
            if file_name not in existing_files:
                print("File %s does not exist" % file_name)
                continue

            # Open file and parse json
            json_data = None
            with open(file_name, "r") as fp:
                json_data = json.load(fp)
            if json_data is None:
                raise RuntimeError()

            # Extract and add data
            parse_data(json_data, station_ids, data_map)

        # Create response object
        response = DataResponse()
        response.station_ids = station_ids
        response.data_map = data_map
        return response


def parse_data(json_list, station_ids, data_map):
    """Given contents of a single data file, update the given data objects."""

    #   See if station is in required region
    #   Check if station is already in map, else add
    #   Check if data point is already in station data_map, else add
    # TODO Do parsing magic


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
    while ts < end_timestamp:
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

    return "netatmo_%s%s%s_%s%s.json" % (
        str(timestamp.year).zfill(4),
        str(timestamp.month).zfill(2),
        str(timestamp.day).zfill(2),
        str(timestamp.hour).zfill(2),
        str(timestamp.minute).zfill(2)
    )
