# Written for python3.4
import json
import sys
from time import time
from urllib.error import URLError
from urllib.request import Request, urlopen

# User modules
from domain.file_io import save_file, save_file_aws
from domain.load_credentials import load_aws_keys, load_key


def main(write_directory, file_name):
    """
    parameters
    ----------
    write_directory: str, directory path to write towith trailing slash
    file_name: str, file name to write to without extension
    """

    query_start = time()
    request_template = 'https://api.netatmo.net/api/getallweatherdata?key=%s'
    key = load_key()
    aws_keys = load_aws_keys()

    # Prepare request
    request_url = request_template % key
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
        if'location' not in point:
            continue
        filtered_json.append(point)

    # Write data
    # print("Writing data to file..")
    # write_start = time()
    # save_file(
    #     filtered_json, write_directory + file_name + ".json.gz")
    # print("Total write time: %fs" % (time() - write_start))

    print("Writing data to S3..")
    write_start = time()
    save_file_aws(
        filtered_json, 'data/' + file_name + ".json.gz", aws_keys)
    print("Total write time: %fs" % (time() - write_start))


if __name__ == "__main__":

    # This program is written in Python 3.4
    if sys.version_info < (3, 0):
        print('Please use Python 3. Exiting.')
        sys.exit(-1)

    # First argument is the directory to write to.
    # Second argument is the file name to write.

    main(sys.argv[1], sys.argv[2])
