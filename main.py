import sys
import numpy as np
from datetime import datetime

# User modules
from domain.base import DataRequest
from domain.fs_engine import (
    FileSystemEngine
)
from domain.elevation_service import (
    ElevationServiceConnector
)

from helpers.utils import (
    get_station_coordinates,
    add_station_elevations
)


def load_global(fspath):
    fsengine = FileSystemEngine(fspath)

    request = DataRequest()
    start_dt = datetime(2016, 3, 24, 00, 00)
    end_dt = datetime(2016, 3, 24, 6, 00)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 360
    print("Querying file system engine..")
    response = fsengine.query(request)
    return response.data_map


def load_netherlands(fspath):
    fsengine = FileSystemEngine(fspath)

    # Defining a request for the Netherlands
    request = DataRequest()
    start_dt = datetime(2016, 3, 24, 00, 00)
    end_dt = datetime(2016, 3, 24, 6, 00)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 360
    request.region = (53.680, 2.865, 50.740, 7.323)
    print("Querying file system engine..")
    response = fsengine.query(request)

    # Augment response with elevation requests.
    print("Qeurying elevation service..")
    elevation_connector = ElevationServiceConnector("http://localhost:8080")
    station_coordinates = get_station_coordinates(response.data_map)
    elevations = elevation_connector.query(station_coordinates)
    station_ids = np.array(station_coordinates)[:, 2]
    add_station_elevations(response.data_map, station_ids, elevations)
    return response.data_map


def load_test(fspath):
    fsengine = FileSystemEngine(fspath)

    request = DataRequest()
    start_dt = datetime(2016, 4, 10, 10, 30)
    end_dt = datetime(2016, 4, 10, 10, 40)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 10
    request.region = (53.680, 2.865, 50.740, 7.323)
    print("Querying file system engine..")
    response = fsengine.query(request)
    return response.data_map


if __name__ == "__main__":
    if len(sys.argv) > 1:
        fspath = sys.argv[1]
    else:
        fspath = 'X:/netatmo/data/'
    # data_map = load_global(fspath)
    # data_map = load_netherlands(fspath)
    data_map = load_test(fspath)

    # test_data_path = 'data/test_data.pkl'
    # import pickle
    # with open(test_data_path, 'rb') as f:
    #     data_map = pickle.load(f)
    #
    # station_ids = []
    # for station in data_map:
    #     if len(data_map[station].forecast_data) > 1:
    #         station_ids.append(station)
