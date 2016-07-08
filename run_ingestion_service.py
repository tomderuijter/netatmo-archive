import logging
from time import time
from datetime import datetime
from domain.base import DataRequest
from domain.ingestion_service import IngestionService

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level='INFO'
    )

    program_start = time()
    test_request = DataRequest()
    start_dt = datetime(2016, 3, 31, 23, 50)
    end_dt = datetime(2016, 5, 1, 0, 0)
    test_request.start_datetime = start_dt
    test_request.end_datetime = end_dt
    test_request.time_resolution = 10
    test_request.region = (53.680, 2.865, 50.740, 7.323)  # The Netherlands
    main_thread = IngestionService()
    main_thread.run(test_request)
    program_end = time()

    logging.info('Program finished (%ds).' % (program_end - program_start))
