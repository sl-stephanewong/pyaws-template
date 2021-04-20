import boto3


class AWSUtil:

    def get_credentials(self):
        return self._session.get_credentials()

    def get_resource_s3(self):
        return self._s3

    def __init__(self):
        self._session = boto3.Session()
        self._s3 = boto3.resource('s3')
