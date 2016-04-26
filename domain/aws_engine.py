from boto3 import Session


class S3Bucket(Session):
    """Connector class for a AWS S3 bucket."""

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key):
        """Initialize an AWS S3 connector for a specific bucket."""
        super(S3Bucket, self).__init__(
            aws_access_key_id,
            aws_secret_access_key,
            region_name='eu-west-1'
        )
        self.bucket = self.resource('s3').Bucket(bucket)

    def read(self, file_path):
        """Download a file from S3."""
        return self.bucket.Object(file_path).get()['Body'].read()

    def write(self, file_path, data):
        """Upload a new file to S3.

        parameters
        ----------
        file_path: str, storage location on bucket.
        data: bytes, data to store on location.
        """
        self.bucket.put_object(
            Key=file_path,
            Body=data
        )

    def delete(self, file_path):
        """Delete a file from S3."""
        self.bucket.Object(file_path).delete()

    def update(self, file_data, *args, **kwargs):
        """Update a file in S3."""
        raise NotImplementedError("Update is not implemented.")
