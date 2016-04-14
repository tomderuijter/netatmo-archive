import matplotlib.pyplot as plt

def plot_temperature(data_map):
    """Draws temperature time series for every station available in data_map"""
    for station_id in list(data_map.keys()):
        station_data = data_map[station_id].forecast_data
        temp = [x.temperature for x in station_data]
        dates = [x.valid_datetime for x in station_data]
        plt.plot(dates, temp, marker='o')
        plt.title("Station id %s" % station_id)
        plt.show()
