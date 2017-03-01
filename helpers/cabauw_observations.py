from urllib.request import urlopen

import pandas as pd
from joblib import Memory

memory = Memory(cachedir='cache/')


def get_cabauw_observations(dates):
    all_dates_data = {}
    for date in dates:
        raw_data = download_data(date)
        single_date_data = parse_data(raw_data)
        all_dates_data = merge_date_data(all_dates_data, single_date_data)
    return all_dates_data


@memory.cache
def download_data(date):
    url = "http://projects.knmi.nl/" \
          "cabauw/bsrn/quicklooks/timeline.php?csrc=bsrn&day=" \
          "{}".format(date.strftime("%Y-%m-%d"))
    data = urlopen(url).read().decode('utf-8')
    return data


def parse_data(data):

    functions = data.split('function')

    data = {}
    for function in functions:
        if function == '':
            continue

        function_name, function_data = parse_function(function)
        data[function_name] = function_data
    return data


def merge_date_data(multi, single):
    if multi == {}:
        return single

    for name in multi:
        df_multi = multi[name]
        df_single = single[name]
        multi[name] = pd.concat([df_multi, df_single], axis=0)
    return multi


def parse_function(function):
    # Split into separate lines
    fun_lines = function.split('+')

    # Remove javascript gunk
    fun_lines = list(map(lambda line: line.strip(" \r\n\"\\n};"), fun_lines))

    # Parse function name
    fun_name = fun_lines[0].split('()')[0]

    # Create dataframe
    fun_data = list(map(lambda l: l.split(','), fun_lines[1:]))
    header = fun_data[0]
    rows = fun_data[1:]
    df = pd.DataFrame(rows, columns=header)

    # Convert to types
    df[header[0]] = pd.to_datetime(df[header[0]])
    df[header[1:]] = df[header[1:]].apply(pd.to_numeric)
    df.set_index(header[0], drop=True, inplace=True)
    df = df.dropna()
    return fun_name, df


if __name__ == "__main__":
    # Example usage
    from datetime import datetime, timedelta
    test_dates = [datetime(2016, 12, 14) + timedelta(days=d) for d in range(3)]
    observations = get_cabauw_observations(test_dates)
