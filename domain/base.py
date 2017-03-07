"""Module with base objects for NetAtmo data processing."""


class DataRequest(object):
    """Simple request class for querying a repository for stations."""
    def __init__(self):
        # Data query start-time in UTC
        self.start_datetime = None
        # Data query end-time in UTC
        self.end_datetime = None
        # Maximum resolution in minutes
        self.time_resolution = None
        # None is world-wide, else provide a tuple four entries marking the
        # top left and lower right lat-lon points of the bounding box.
        self.region = None


class DataResponse(object):
    """Simple response class as result of querying file system."""
    def __init__(self):
        # Map from station ids to station objects
        self.data_map = {}


class Station(object):

    binary_subtype = 128

    def __init__(self, station_id, lat, lon):
        self.station_id = station_id
        self.latitude = lat
        self.longitude = lon
        self.elevation = None

        self.thermo_module = {
            'valid_datetime': [],
            'temperature': [],
            'humidity': [],
            'pressure': []
        }

        self.hydro_module = {
            'time_day_rain': [],
            'time_hour_rain': [],
            'daily_rain_sum': [],
            'hourly_rain_sum': []
        }

    @classmethod
    def from_dict(cls, d):
        station_id = d['_id']
        latitude = d['latitude']
        longitude = d['longitude']
        station = Station(station_id, latitude, longitude)
        station.elevation = d['elevation']
        station.thermo_module = d['thermo_module']
        station.hydro_module = d['hydro_module']
        return station
