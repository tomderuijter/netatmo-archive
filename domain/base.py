class DataRequest(object):

    def init(self):
        # Data query start-time in UTC
        self.start_date_time = None
        # Data query end-time in UTC
        self.end_date_time = None
        # Maximum resolution in minutes
        self.time_resolution = None
        # None is world-wide, else provide a list with two tuples
        self.region = None


class DataResponse(object):

    def init(self):
        # List of included station ids
        self.station_ids = []
        # Map from station ids to station objects
        self.data_map = {}


class Station(object):

    def init(self):
        self.station_id = None
        self.latitude = None
        self.longitude = None
        self.elevation = None
        self.meta_data = None
        # List of ObservationData objects by valid_date_time in ascending order
        self.forecast_data = []


class ObservationData(object):

    def init(self):
        # All times are in UTC
        self.valid_date_time = None
        # Other objects are optional
