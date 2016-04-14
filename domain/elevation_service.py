import json
from time import time, sleep
from urllib.request import Request, urlopen
from urllib.error import URLError

"""Module for querying the elevation service"""


class JsonContentException(Exception):
    pass


class ElevationServiceConnector(object):

    def __init__(self, url):
        """Initialization of connection with elevation service

        parameters
        ----------
        url: str, connection url
        """
        self.request_template = url + '/elevation-debug/%s/%s/'

    def get_query_url(self, latitude, longitude):
        return self.request_template % (latitude, longitude)

    def query(self, points):
        """Given a list of lists of lat-lon values, return a list of elevations

        parameters: list, list of lat-lon lists.
        """
        print("Querying microservice..")
        query_start = time()
        nr_requests = len(points)
        elevations = [None] * nr_requests
        bad_requests = 0
        # Loop over coordinates
        for c in range(0, nr_requests):
            if c % 200 == 0 and c != 0:
                print('Processed %d / %d requests (%ds).' %
                      (c, nr_requests, time() - query_start))
            # Prepare request
            request_url = self.get_query_url(points[c][0], points[c][1])
            request = Request(request_url)

            try:
                # Do HTTPRequest
                response = urlopen(request)
                content = response.read().decode('utf-8')
                # Parse JSON.
                result = json.loads(content)
                if 'properties' not in result:
                    raise JsonContentException("1Elevation data not available")

                if 'elevationDataSource' not in result['properties']:
                    raise JsonContentException("2Elevation data not available")

                if result['properties']['elevationDataSource'] == "unknown":
                    raise JsonContentException("3Elevation data not available")

                if 'elevationInMeter' not in result['properties']:
                    raise JsonContentException("4Elevation data not available")

                elevations[c] = int(result['properties']['elevationInMeter'])

                # Overload protection
                # sleep(0.01)

            except URLError as e:
                print('HTTP Error:', e)
                bad_requests += 1
                continue
            except JsonContentException as e:
                print("JSON Content error:", e)
                bad_requests += 1
                continue

        print("Done querying microservice (%ds)." % (time() - query_start))
        print("Skipped %d / %d lat-lon pairs." % (bad_requests, nr_requests))
        return elevations
