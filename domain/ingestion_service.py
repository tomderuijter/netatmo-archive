"""Module for ingesting NetAtmo data into MongoDB."""
import math
import logging
import multiprocessing as mp
from datetime import datetime
from time import sleep

# User modules
from domain.json_parser import parse_stations, log_parse_stats
from domain.load_credentials import load_aws_keys
from domain.file_io import (list_requested_files, load_file_aws)
from domain.base import DataRequest
from domain.mongodb_engine import MongoDBConnector


class MultiProcessingTest(object):
    """Module for ingesting files from S3 into a MongoDB."""

    def __init__(self):
        self.s3_connections = 2  # mp.cpu_count()
        self.db_connections = 4  # mp.cpu_count()

        self.file_consumer_count = 2  # mp.cpu_count()
        self.json_consumer_count = 4  # mp.cpu_count()

        self._file_queue = None
        self._json_queue = None
        self._s3_semaphore = None
        self._db_semaphore = None

    def run(self, request):
        """Download, ingest and upload files from S3 to MongoDB."""
        logging.info("Main thread: initializing ingestion process.")
        self._file_queue = mp.JoinableQueue()
        self._json_queue = mp.JoinableQueue()

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

    def _add_files_to_queue(self, files_to_load):
        _add_to_queue(self._file_queue, files_to_load)

    def _start_file_consumers(self, request):
        for _ in range(self.file_consumer_count):
            FileConsumer(
                self._s3_semaphore, self._file_queue, self._json_queue, request
            ).start()

    def _close_file_queue(self):
        _add_to_queue(
            self._file_queue,
            [PoisonPill(x + 1000) for x in range(self.file_consumer_count)])
        self._file_queue.close()
        self._file_queue.join_thread()

    def _start_json_consumers(self):
        for _ in range(self.json_consumer_count):
            JSONConsumer(self._db_semaphore, self._json_queue).start()

    def _close_json_queue(self):
        _add_to_queue(
            self._json_queue,
            [PoisonPill(x) for x in range(self.json_consumer_count)])
        self._json_queue.close()
        self._json_queue.join_thread()


class FileConsumer(mp.Process):
    """Consumer process for loading and parsing files from S3."""

    def __init__(self, s3_semaphore, input_queue, output_queue, request):
        super().__init__()
        self.s3_semaphore = s3_semaphore
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.request = request

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
                # TODO do catch error if file is not found.
                file_contents = _download_from_s3(next_task)
            assert file_contents is not None

            station_mapping = _json_to_station_objects(file_contents, self.request.region)
            logging.debug("%s: %d stations in file." % (self.name, len(station_mapping)))
            logging.info("%s: finished task." % self.name)
            self.input_queue.task_done()

            # Split dictionary in chunks for distributed ingestion
            station_mapping_parts = _split_dictionary(station_mapping)
            # TODO TdR 06/07/16: Debug _split_dictionary.
            _add_to_queue(self.output_queue, station_mapping_parts)
            logging.info('%s: placed %d tasks on output queue.' % (self.name, len(station_mapping_parts)))
        return


class JSONConsumer(mp.Process):
    """Consumer process for pushing station objects into MongoDB."""

    def __init__(self, db_semaphore, input_queue):
        super().__init__()
        self.db_semaphore = db_semaphore
        self.input_queue = input_queue

    def run(self):
        """Push object mapping into MongoDB."""
        logging.info("%s: starting." % self.name)
        while True:
            next_task = self.input_queue.get()
            print("%s: selected task type %s" % (self.name, type(next_task)))
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
    aws_keys = load_aws_keys()
    return load_file_aws(file_path, aws_keys)


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
def _split_dictionary(whole_dict, chunk_size=5000):
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

    # Defining a request for the Netherlands
    test_request = DataRequest()
    start_dt = datetime(2016, 4, 1, 00, 00)
    end_dt = datetime(2016, 4, 1, 00, 00)
    test_request.start_datetime = start_dt
    test_request.end_datetime = end_dt
    test_request.time_resolution = 10
    # test_request.region = (52.000, 4.790, 51.880, 5.080)
    test_request.region = (53.680, 2.865, 50.740, 7.323)
    main_thread = MultiProcessingTest()
    main_thread.run(test_request)
