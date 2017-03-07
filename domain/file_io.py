import gzip
import json
import os
from datetime import timedelta, datetime

from domain.aws_engine import S3Bucket
from domain.base import DataRequest, DataResponse
from domain.json_parser import parse_stations, log_parse_stats
from helpers import utils


def save_file(obj, file_path):
    """Compress and store a json object to disk."""
    with gzip.open(file_path, "wb") as fp:
        fp.write(json.dumps(obj).encode('utf-8'))


def load_file(file_path):
    """Load a compressed json file from disk."""
    with gzip.open(file_path, "rb") as fp:
        return json.loads(fp.read().decode('utf-8'))


def save_file_aws(obj, file_path, aws_credentials):
    """Compress and store a json object or dictionary to an S3 bucket."""
    bucket_engine = S3Bucket(*aws_credentials)
    data = gzip.compress(json.dumps(obj).encode('utf-8'))
    bucket_engine.write(file_path, data)


def load_file_aws(file_path, aws_credentials):
    """Load a compressed json file from S3."""
    bucket_engine = S3Bucket(*aws_credentials)
    return json.loads(
        gzip.decompress(
            bucket_engine.read(file_path)
        ).decode('utf-8')
    )


def ls_json(directory):
    """List json file objects in engine directory."""
    return_list = [
        f for f in os.listdir(directory) if f.endswith(".json.gz")
        ]
    return_list.sort()
    return return_list


def query(root_directory, request):
    """Query the file system."""
    assert isinstance(request, DataRequest)

    # Initialize data objects
    data_map = {}

    request_file_names = list_requested_files(request)
    existing_files = ls_json(root_directory)

    # Load request files
    print("Loading %d files in total." % (len(request_file_names)))
    for (count, file_name) in enumerate(request_file_names):
        print("File %d: %s" % (count + 1, file_name))
        # File does not exist.
        if file_name not in existing_files:
            print("File does not exist\n")
            continue

        # Open file and parse json
        json_data = load_file(root_directory + file_name)
        if json_data is None:
            raise RuntimeError()

        # Extract and add data
        parse_stats = \
            parse_stations(json_data, data_map, request.region)

        log_parse_stats(parse_stats)

    utils.add_alias(data_map)

    response = DataResponse()
    response.data_map = data_map
    return response


def list_requested_files(request):
    """List files to ingest to comply with the request."""
    request_datetime_range = datetime_range(
        request.start_datetime,
        request.end_datetime,
        request.time_resolution
    )
    # Translate request datetime into files
    request_file_names = [
        datetime_to_file_name(ts) for ts in request_datetime_range
    ]
    return request_file_names


def datetime_range(start_timestamp, end_timestamp, step_size):
    """Range function, specified for datetime.datetime classes.

    parameters
    ----------
    start_timestamp: datetime.datetime, in UTC
    end_timestamp: datetime.datetime, in UTC
    step_size: int, step size in minutes
    """
    r = []
    ts = start_timestamp
    while ts <= end_timestamp:
        r.append(ts)
        ts += timedelta(minutes=step_size)
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
