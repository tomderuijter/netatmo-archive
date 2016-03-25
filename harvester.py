# Written for python3.4
import sys
import json
from domain.fs_engine import save_file
from time import time
from urllib.request import Request, urlopen
from urllib.error import URLError


def load_key():
    with open("config/api_key", "r") as file:
        return file.readlines()[0].strip()


def main(write_dir, file_name):
    """
    parameters
    ----------
    write_dir: str, directory path to write towith trailing slash
    file_name: str, file name to write to without extension
    """

    query_start = time()
    request_template = 'https://api.netatmo.net/api/getallweatherdata?key=%s'
    # TODO Load key from file.
    key = load_key()

    # Prepare request
    request_url = request_template % (key)
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
        if not ('location' in point and inside_box(*point['location'])):
            continue
        filtered_json.append(point)

    # Write data

    print("Writing data to file..")
    write_start = time()
    save_file(filtered_json, write_dir, file_name + ".json.gz")
    print("Total write time: %fs" % (time() - write_start))


def inside_box(lat, lon):
    # TODO Define.
    return True


if __name__ == "__main__":

    # This program is written in Python 3.4
    if (sys.version_info < (3, 0)):
        print('Please use Python 3. Exiting.')
        sys.exit(-1)

    # First argument is the directory to write to.
    write_dir = sys.argv[1]

    # Second argument is the file name to write.
    file_name = sys.argv[2]

    main(write_dir, file_name)
