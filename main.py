import sys
import numpy as np
from datetime import datetime

# User modules
from domain.base import DataRequest
import domain.fs_engine as netatmo
from domain.elevation_service import (
    ElevationServiceConnector
)

from helpers.utils import (
    get_station_coordinates,
    add_station_elevations
)
import helpers.knmi_obs_ingest as knmi


def load_global(fspath):
    netatmo_fsengine = netatmo.FileSystemEngine(fspath)

    # Define request
    request = DataRequest()
    start_dt = datetime(2016, 3, 24, 00, 00)
    end_dt = datetime(2016, 3, 24, 6, 00)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 360
    print("Querying file system engine..")
    response = netatmo_fsengine.query(request)
    return response.data_map


def load_netherlands(fspath):
    netatmo_fsengine = netatmo.FileSystemEngine(fspath)

    # Defining a request for the Netherlands
    request = DataRequest()
    start_dt = datetime(2016, 3, 25, 00, 00)
    end_dt = datetime(2016, 3, 25, 1, 00)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 10
    request.region = (53.680, 2.865, 50.740, 7.323)
    # Query netatmo data
    print("Querying NetAtmo file system engine..")
    response = netatmo_fsengine.query(request)
    return response.data_map


def load_knmi_obs(fspath):
    knmi_fsengine = knmi.FileSystemEngine(fspath)

    # Define request
    request = DataRequest()
    start_dt = datetime(2016, 3, 25, 00, 00)
    end_dt = datetime(2016, 3, 25, 1, 00)
    request.start_datetime = start_dt
    request.end_datetime = end_dt
    request.time_resolution = 10
    print("Querying KNMI observation data..")
    obs_data = knmi_fsengine.query(request)
    return obs_data


def load_elevation(data_map):
    # Augment response with elevation requests.
    print("Qeurying elevation service..")
    elevation_connector = ElevationServiceConnector("http://localhost:8080")
    station_coordinates = get_station_coordinates(data_map)
    elevations = elevation_connector.query(station_coordinates)
    station_ids = np.array(station_coordinates)[:, 2]
    add_station_elevations(data_map, station_ids, elevations)
    return data_map


if __name__ == "__main__":
    if len(sys.argv) > 1:
        fspath = sys.argv[1]
    else:
        fspath = 'X:/netatmo/data/'
    # data_map = load_global(fspath)
    data_map = load_netherlands(fspath)
    #
    # station_ids = []
    # for station in data_map:
    #     if data_map[station].thermo_module is not None and \
    #        len(data_map[station].thermo_module) > 1:
    #         station_ids.append(station)

    # Load observations
    obs_data = load_knmi_obs('X:/netatmo/obs/')
