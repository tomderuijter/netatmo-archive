"""Module for ingesting NetAtmo data into MongoDB."""
import multiprocessing as mp


class MultiProcessingTest(object):
    """Module for ingesting files from S3 into a MongoDB."""

    def __init__(self):
        self.s3_connections = 2
        self.db_connections = 2

        self.file_consumers = mp.cpu_count()
        self.json_consumers = mp.cpu_count()

    def run(self, request):
        print("Main thread: initializing ingestion process.")
        file_queue = mp.JoinableQueue()
        json_queue = mp.JoinableQueue()

        s3_semaphore = mp.BoundedSemaphore(self.s3_connections)
        db_semaphore = mp.BoundedSemaphore(self.db_connections)

        files_to_load = get_request_file_paths(request)
        add_to_queue(file_queue, files_to_load)
        add_to_queue(file_queue, [PoisonPill()] * self.file_consumers)

        print("Main thread: starting worker processes.")
        # TODO Move this into separate function
        file_consumers = [
            FileConsumer(s3_semaphore, file_queue, json_queue)
            for x in range(self.file_consumers)]
        for w in file_consumers:
            w.start()

        # TODO Move to separate function
        json_consumers = [
            JSONConsumer(db_semaphore, json_queue)
            for x in range(self.json_consumers)]
        for w in json_consumers:
            w.start()

        # Wait for file loading to finish, then decomission json consumers.
        file_queue.close()
        file_queue.join_thread()
        add_to_queue(json_queue, [PoisonPill()] * self.json_consumers)
        print("Main thread: downloading complete.")
        json_queue.close()
        json_queue.join_thread()
        print("Main thread: database ingestion complete.")


class FileConsumer(mp.Process):
    """Consumer process for loading and parsing files from S3."""

    def __init__(self, file_semaphore, file_queue, result_queue):
        super().__init__()
        self.file_semaphore = file_semaphore
        self.file_queue = file_queue
        self.result_queue = result_queue

    def run(self):
        print("%s: starting." % self.name)
        while True:
            next_task = self.file_queue.get()
            if isinstance(next_task, PoisonPill):
                print("%s: exiting." % self.name)
                self.file_queue.task_done()
                break

            file_contents = None
            with self.file_semaphore:
                print("%s: downloading file from S3." % self.name)
                file_contents = download_from_S3(next_task)
            assert file_contents is not None

            station_mapping = map_to_station_objects(file_contents)

            print("%s: finished task." % self.name)
            self.file_queue.task_done()
            self.result_queue.put(station_mapping)
        return


class JSONConsumer(mp.Process):
    """Consumer process for pushing objects into MongoDB."""

    def __init__(self, db_semaphore, json_queue):
        super().__init__()
        self.db_semaphore = db_semaphore
        self.json_queue = json_queue

    def run(self):
        print("%s: starting." % self.name)
        while True:
            next_task = self.json_queue.get()
            if isinstance(next_task, PoisonPill):
                print("%s: exiting." % self.name)
                self.json_queue.task_done()
                break

            with self.db_semaphore:
                print("%s: opening database connection." % self.name)
                # Make database connection.
                # For new stations in task: insert.
                # For existing stations in task:
                #   append measurements.
                #   if possible, insert measurements at specific place to
                #     enfore sorted time series.
                #   overwrite coordinates if changed
                # TODO
            print("%s: finished task." % self.name)
            self.json_queue.task_done()
        return


class PoisonPill(object):
    """Construct to signal a process to stop."""


def get_request_file_paths(request):
    # TODO
    return range(10)


def add_to_queue(queue, tasks):
    for task in tasks:
        queue.put(task)


def download_from_S3(file_path):
    # TODO
    return "Lorem ipsum dolor sit amet."


def map_to_station_objects(json_string):
    # TODO
    return "Foo"


if __name__ == "__main__":
    request = None
    main_thread = MultiProcessingTest()
    main_thread.run(request)
