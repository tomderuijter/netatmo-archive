class DataRequest(object):

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

    def __init__(self):
        # Map from station ids to station objects
        self.data_map = {}


class Station(object):

    def __init__(self, station_id, lat, lon):
        self.station_id = station_id
        self.latitude = lat
        self.longitude = lon
        self.elevation = None

        self.thermo_module = None
        self.wind_module = None
        self.precip_module = None
