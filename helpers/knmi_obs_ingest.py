import pandas as pd
from datetime import datetime, timedelta, timezone

# User modules
from domain.file_io import datetime_range

# Important parameters names
parameter_name_to_value = {
    # Temperature group
    'ta': 'temperature_10min_celcius',
    'tb': 'web_bulb_10min_celcius',
    'td': 'dewpoint_1min_celcius',
    'td10': 'dewpoint_10min_celcius',
    # Humidity
    'rh': 'relative_humidity_1min_perc',
    'rh10': 'relative_humidity_10min_perc',
    # Wind group
    'ff': 'wind_speed_10min_mps',
    'dd': 'wind_dir_10min_deg',
    # Precipitation group
    # TODO
    # Cloudiness group
    'h': 'cloud_base_ft',
    'h1': 'cloud_base1_ft',
    'h2': 'cloud_base2_ft',
    'h3': 'cloud_base3_ft',
    'n': 'total_cloud_cover_octa',
    # Pressure group
    'p0': 'pressure_station_1min_hpa',
    'pp': 'pressure_sea_1min_hpa',
    'ps': 'pressure_sensor_1min_hpa'
    # Radiation group
    # TODO
}


class FileSystemEngine(object):

    def __init__(self, dir_path):
        self.directory = dir_path

    def query(self, request):

        # Given request, generate the file names to load
        slack = timedelta(minutes=20)
        request_datetime_range = datetime_range(
            request.start_datetime - slack,
            request.end_datetime + slack,
            request.time_resolution
        )
        request_file_names = [
            datetime_to_file_name(ts) for ts in request_datetime_range
        ]

        # Load request files
        data = None
        print("Loading %d files in total." % (len(request_file_names)))
        for (count, file_name) in enumerate(request_file_names):
            print("File %d: %s" % (count + 1, file_name))

            try:
                file_data = read_obs_file(self.directory + file_name)
                # Append data
                if data is None:
                    data = file_data
                else:
                    data = data.append(file_data)
            except OSError:
                print("File not found: %s" % file_name)
                continue
        if data is not None:
            data.sort_values('valid_datetime', axis=0, inplace=True)
        return data


def date_parser(date, time):
    """yyyymmdd and hh strings to UTC datetime object.

    parameters
    ----------
    date: string in yyyymmdd format.
    time: string in hhMM format.
    """
    if not(isinstance(date, str) and isinstance(time, str)):
        raise ValueError("Non-string arguments passed.")
    year, month, day = (int(date[0:4]), int(date[4:6]), int(date[6:8]))
    hour, minute = (int(time[0:2]), int(time[2:]))
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def read_obs_file(file_path):
    column_names = [
        'station_id', 'valid_date', 'valid_time', 'element_name', 'value'
    ]
    df = pd.read_csv(
        file_path,
        header=None,
        names=column_names,
        usecols=[0, 1, 2, 4, 5],
        na_values=['', '              '],
        parse_dates={'valid_datetime': [1, 2]},
        skipinitialspace=True
    )
    return df


def datetime_to_file_name(timestamp):
    """Converts a datetime timestamp to the necessary file name.
    parameters
    ----------
    timestamp: datetime.datetime object
    """
    assert isinstance(timestamp, datetime)
    return "o%s%s%s%s.txt" % (
        str(timestamp.month).zfill(2),
        str(timestamp.day).zfill(2),
        str(timestamp.hour).zfill(2),
        str(timestamp.minute).zfill(2)
    )
