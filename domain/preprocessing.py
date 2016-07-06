import pandas as pd


# TODO Really really slow. Works well for large amounts per station.
def resample_and_interpolate(data_map, resolution=10):
    """Resample and interpolate a dictionary to pandas dataframe."""
    for count, station_id in enumerate(data_map):
        if count % 1000 == 0:
            print("%d / %d stations processed.." % (count + 1, len(data_map)))

        station = data_map[station_id]

        if station.thermo_module is not None and \
           type(station.thermo_module) is dict:
            df = pd.DataFrame(station.thermo_module)
            df.set_index('valid_datetime', drop=True, inplace=True)
            interpolation_limit = 3 if resolution <= 20 else 0
            df = df.resample(str(resolution) + 'T').mean().interpolate(
                method='time',
                limit=interpolation_limit
            )
        else:
            df = pd.DataFrame()
        station.thermo_module = df
    print("Done.")