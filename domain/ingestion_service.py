"""Module for ingesting NetAtmo data into MongoDB."""

"""
- Assume empty database

- Receive request for:
    - Start time
    - End time
    - Resolution
    - Area

- List necessary files.

- Two asynchronous queues:
    1.
    - Load file from S3.
    - Ingest file.
    - Filter file.

    2.
    - Push ingested station data to MongoDB.

"""
