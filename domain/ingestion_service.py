"""Module for ingesting NetAtmo data into MongoDB."""
import math
import logging
import multiprocessing as mp
from datetime import datetime
from time import time, sleep
import botocore

# User modules
from domain.json_parser import parse_stations, log_parse_stats
from domain.load_credentials import load_aws_keys
from domain.file_io import (list_requested_files, load_file_aws)
from domain.base import DataRequest
from domain.mongodb_engine import MongoDBConnector


class IngestionService(object):
    """Module for ingesting files from S3 into a MongoDB."""

    def __init__(self):

        self.s3_connections = 2
        # The maximum number of s3 connections depend on the clients bandwidth
        # to the AWS servers.
        self.db_connections = 4
        # While concurrent writing is in theory not faster, a small number of
        # threads yield a small write performance increase.

        self.file_consumer_count = 2
        self.json_consumer_count = 4

        self._file_queue = None
        self._json_queue = None
        self._error_queue = None
        self._s3_semaphore = None
        self._db_semaphore = None

    def run(self, request):
        """Download, ingest and upload files from S3 to MongoDB."""
        logging.info("Main thread: initializing ingestion process.")
        # All files are queued at the same time. No limit required.
        self._file_queue = mp.JoinableQueue()
        # Once a file is downloaded, it is split up and put on the json queue.
        # Limiting the json queue is required to match download speed with ingestion speed.
        self._json_queue = mp.JoinableQueue(mp.cpu_count() * 2)
        self._error_queue = mp.SimpleQueue()

        self._s3_semaphore = mp.BoundedSemaphore(self.s3_connections)
        self._db_semaphore = mp.BoundedSemaphore(self.db_connections)

        files_to_load = _get_request_file_paths(request)
        logging.info(
            "Main thread: %d files to download." %
            len(files_to_load))
        self._add_files_to_queue(files_to_load)

        logging.info(
            "Main thread: starting %d file loader processes." %
            self.file_consumer_count)
        self._start_file_consumers(request)
        logging.info("Main thread: all tasks listed in file queue.")
        logging.info("Main thread: queueing poison pills for file loaders.")
        self._close_file_queue()

        logging.info(
            "Main thread: starting %d database ingestion processes." %
            self.json_consumer_count)
        self._start_json_consumers()

        # Wait for file queue to be empty, so that all ingestion tasks are queued.
        self._file_queue.join()  # Block main thread
        logging.info("Main thread: downloading complete.")
        logging.info("Main thread: all ingestion tasks posted.")

        # Wait for existing json tasks to finish before closing the queue.
        # This ensures json consumers are not stopped prematurely.
        self._json_queue.join()  # Blocking operation
        logging.info("Main thread: queueing poison pills for database ingesters.")
        # Shouldn't be called until the json_queue is completely empty.
        self._close_json_queue()
        logging.info("Main thread: database ingestion complete.")

        # TODO TdR 18/07/16: do something with error queue contents.

    def _add_files_to_queue(self, files_to_load):
        _add_to_queue(self._file_queue, files_to_load)

    def _start_file_consumers(self, request):
        for _ in range(self.file_consumer_count):
            FileConsumer(
                self._s3_semaphore,
                self._file_queue, self._json_queue, self._error_queue,
                request, self.json_consumer_count
            ).start()

    def _close_file_queue(self):
        _add_to_queue(
            self._file_queue,
            [PoisonPill(x + 1000) for x in range(self.file_consumer_count)])
        self._file_queue.close()
        self._file_queue.join_thread()

    def _start_json_consumers(self):
        for _ in range(self.json_consumer_count):
            JSONConsumer(self._db_semaphore, self._json_queue, self._error_queue).start()

    def _close_json_queue(self):
        _add_to_queue(
            self._json_queue,
            [PoisonPill(x) for x in range(self.json_consumer_count)])
        self._json_queue.close()
        self._json_queue.join_thread()


class FileConsumer(mp.Process):
    """Consumer process for loading and parsing files from S3."""

    def __init__(self, s3_semaphore, input_queue, output_queue, error_queue, request, worker_count):
        super().__init__()
        self.s3_semaphore = s3_semaphore
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.error_queue = error_queue
        self.request = request
        self.worker_count = worker_count

    def run(self):
        logging.info("%s: starting." % self.name)
        while True:
            next_task = self.input_queue.get()
            if isinstance(next_task, PoisonPill):
                logging.info("%s: encountered %s. Exiting." % (self.name, next_task))
                self.input_queue.task_done()
                break

            with self.s3_semaphore:
                logging.info("%s: downloading file S3://%s" % (self.name, next_task))
                try:
                    file_contents = _download_from_s3(next_task)
                except botocore.exceptions.EndpointConnectionError as e:
                    error_msg = "%s: network error. Could not download file %s." % (self.name, next_task)
                    logging.error(error_msg)
                    self.error_queue.put((error_msg, e))
                    self.input_queue.task_done()
                    continue
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        # File does not exist on Amazon side.
                        error_msg = "%s: file '%s' does not exist. Continuing." % (self.name, next_task)
                        logging.error(error_msg)
                        self.error_queue.put((error_msg, e))
                        self.input_queue.task_done()
                        continue
                    else:
                        raise

            station_mapping = _json_to_station_objects(file_contents, self.request.region)
            logging.info("%s: finished task." % self.name)
            self.input_queue.task_done()

            # Split dictionary in chunks for distributed ingestion
            minimum_chunk_size = 20000
            chunk_size = max(int(math.ceil(len(station_mapping) / self.worker_count)), minimum_chunk_size)
            station_mapping_parts = _split_dictionary(station_mapping, chunk_size)
            # TODO TdR 06/07/16: Debug _split_dictionary.
            _add_to_queue(self.output_queue, station_mapping_parts)
            logging.info('%s: placed %d stations in %d tasks on output queue.' %
                         (self.name, len(station_mapping), len(station_mapping_parts)))
        return


class JSONConsumer(mp.Process):
    """Consumer process for pushing station objects into MongoDB."""

    def __init__(self, db_semaphore, input_queue, error_queue):
        super().__init__()
        self.db_semaphore = db_semaphore
        self.input_queue = input_queue
        self.error_queue = error_queue

    def run(self):
        """Push object mapping into MongoDB."""
        logging.info("%s: starting." % self.name)
        while True:
            next_task = self.input_queue.get()
            if isinstance(next_task, PoisonPill):
                logging.info("%s: encountered %s. Exiting." % (self.name, next_task))
                self.input_queue.task_done()
                break

            with self.db_semaphore:
                logging.info("%s: opening database connection." % self.name)
                logging.info("%s: bulk update for %d stations." % (self.name, len(next_task)))
                _store_stations_in_database(next_task)
            logging.info("%s: finished task." % self.name)
            self.input_queue.task_done()
        return


class PoisonPill(object):
    """Object equivalent of SIGTERM."""
    def __init__(self, number):
        self.identifier = number

    def __str__(self):
        return 'PoisonPill-%d' % self.identifier


def _get_request_file_paths(request):
    return list_requested_files(request)


def _add_to_queue(queue, tasks):
    for task in tasks:
        queue.put(task)


def _download_from_s3(file_path):
    attempts = 0
    aws_keys = load_aws_keys()
    while True:
        try:
            file_contents = load_file_aws(file_path, aws_keys)
            return file_contents
        except botocore.exceptions.EndpointConnectionError:
            # if attempts < 3:
                logging.error("Connection failure while downloading %s. Trying again in 10 seconds." % file_path)
                attempts += 1
                sleep(10)
            # else:
            #     raise

def _json_to_station_objects(json_object, region):
    data_map = {}
    parse_stats = parse_stations(json_object, data_map, region)
    log_parse_stats(parse_stats)
    return data_map


def _store_stations_in_database(station_dict):
    db_connector = MongoDBConnector()
    db_connector.upsert_stations(station_dict)
    db_connector.close()


# TODO TdR 06/07/16: Test
def _split_dictionary(whole_dict, chunk_size=2500):
    chunk_generator = _chunks(list(whole_dict.items()), chunk_size)
    return [dict(chunk) for chunk in chunk_generator]


def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level='INFO'
    )

    import os
    os.chdir('../')
    print(os.getcwd())

    test_request = DataRequest()
    test_request.start_datetime = datetime(2016, 4, 1, 0, 0)
    test_request.end_datetime = datetime(2016, 5, 1, 0, 0)
    test_request.time_resolution = 10

    program_start = time()
    main_thread = IngestionService()
    main_thread.run(test_request)
    program_end = time()
    logging.info('Program finished (%ds).' % (program_end - program_start))
