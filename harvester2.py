"""Module for requesting data from the NetAtmo partner API."""
import sys
import json
from time import time
from urllib.request import Request, urlopen
from urllib.error import URLError

# User modules
from domain.fs_engine import save_file


def _load_netatmo_key():
    with open("config/api_key", "r") as f:
        return f.readlines()[0].strip()


def _load_aws_keys():
    lines = None
    with open("config/AWS_key", "r") as f:
        lines = f.readlines()
        aws_access_key = lines[0].split('=')[1].strip()
        aws_secret_key = lines[1].split('=')[1].strip()
        return (aws_access_key, aws_secret_key)
    raise IOError("AWS identity could not be loaded.")


def write_to_s3():
    """Foo."""
    pass


def harvest_netatmo_data(write_dir, file_name):
    """
    Load data from netatmo and store it.

    parameters
    ----------
    write_dir: str, directory path to write towith trailing slash
    file_name: str, file name to write to without extension
    """
    query_start = time()
    request_template = 'https://api.netatmo.net/api/getallweatherdata?key=%s'
    netatmo_key = _load_netatmo_key()

    # Prepare request
    request_url = request_template % (netatmo_key)
    request = Request(request_url)

    try:
        # Do HTTPRequest
        response = urlopen(request)
        content = response.read().decode('utf-8')
        print("Total query time: %fs" % (time() - query_start))

    except URLError as e:
        print('Error in downloading world data:', e)
        content = None

    # Parse json
    parsed_json = json.loads(content)

    # Filter coordinates
    filtered_json = []
    for point in parsed_json:
        if not ('location' in point):
            continue
        filtered_json.append(point)

    # Write data

    print("Writing data to file..")
    write_start = time()
    save_file(filtered_json, write_dir, file_name + ".json.gz")
    print("Total write time: %fs" % (time() - write_start))


if __name__ == "__main__":

    if (sys.version_info < (3, 0)):
        print('Please use Python 3. Exiting.')
        sys.exit(-1)

    # First argument is the directory to write to.
    write_dir = sys.argv[1]

    # Second argument is the file name to write.
    file_name = sys.argv[2]

    # harvest_netatmo_data(write_dir, file_name)
