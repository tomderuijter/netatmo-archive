"""Module for ingesting NetAtmo data into MongoDB."""
import logging
import multiprocessing as mp


class MultiProcessingTest(object):
    """Module for ingesting files from S3 into a MongoDB."""

    def __init__(self):
        self.s3_connections = mp.cpu_count()
        self.db_connections = mp.cpu_count()

        self.file_consumer_count = mp.cpu_count()
        self.json_consumer_count = mp.cpu_count()

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
        self._start_file_consumers()

        logging.info(
            "Main thread: starting %d database ingestion processes." %
            self.json_consumer_count)
        self._start_json_consumers()

        self._stop_file_consumers()  # Blocking operation
        logging.info("Main thread: downloading complete.")

        self._stop_json_consumers()  # Blocking operation
        logging.info("Main thread: database ingestion complete.")

    def _add_files_to_queue(self, files_to_load):
        _add_to_queue(self._file_queue, files_to_load)

    def _start_file_consumers(self):
        for _ in range(self.file_consumer_count):
            FileConsumer(
                self._s3_semaphore, self._file_queue, self._json_queue
            ).start()

    def _stop_file_consumers(self):
        _add_to_queue(
            self._file_queue,
            [PoisonPill()] * self.file_consumer_count)
        self._file_queue.close()
        self._file_queue.join_thread()

    def _start_json_consumers(self):
        for _ in range(self.json_consumer_count):
            JSONConsumer(self._db_semaphore, self._json_queue).start()

    def _stop_json_consumers(self):
        _add_to_queue(
            self._json_queue,
            [PoisonPill()] * self.json_consumer_count)
        self._json_queue.close()
        self._json_queue.join_thread()


class FileConsumer(mp.Process):
    """Consumer process for loading and parsing files from S3."""

    def __init__(self, s3_semaphore, input_queue, output_queue):
        super().__init__()
        self.s3_semaphore = s3_semaphore
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        logging.info("%s: starting." % self.name)
        while True:
            next_task = self.input_queue.get()
            if isinstance(next_task, PoisonPill):
                logging.info("%s: exiting." % self.name)
                self.input_queue.task_done()
                break

            with self.s3_semaphore:
                logging.info("%s: downloading file from S3." % self.name)
                file_contents = _download_from_S3(next_task)
            assert file_contents is not None

            station_mapping = _json_to_station_objects(file_contents)

            logging.info("%s: finished task." % self.name)
            self.input_queue.task_done()
            self.output_queue.put(station_mapping)
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
            if isinstance(next_task, PoisonPill):
                logging.info("%s: exiting." % self.name)
                self.input_queue.task_done()
                break

            with self.db_semaphore:
                logging.info("%s: opening database connection." % self.name)
                _store_stations_in_database(next_task)
            logging.info("%s: finished task." % self.name)
            self.input_queue.task_done()
        return


class PoisonPill(object):
    """Object equivalent of SIGTERM."""


def _get_request_file_paths(request):
    # TODO
    return range(10)


def _add_to_queue(queue, tasks):
    for task in tasks:
        queue.put(task)


def _download_from_S3(file_path):
    # TODO
    return "Lorem ipsum dolor sit amet."


def _json_to_station_objects(json_string):
    # TODO
    return "Foo"


def _store_stations_in_database(station_dict):
    # Make database connection.
    # For new stations in task: insert.
    # For existing stations in task:
    #   append measurements.
    #   if possible, insert measurements at specific place to
    #     enfore sorted time series.
    #   overwrite coordinates if changed
    # Close database connection
    # TODO
    pass


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level='INFO'
    )
    test_request = None
    main_thread = MultiProcessingTest()
    main_thread.run(test_request)
