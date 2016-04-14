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
        # List of ObservationData objects by valid_date_time in ascending order
        self.forecast_data = []


class ObservationData(object):

    def __init__(self):
        # All times are in UTC
        self.valid_datetime = None
        self.temperature = None
        self.humidity = None
        self.pressure = None
        # TODO Now for other measurement modules
