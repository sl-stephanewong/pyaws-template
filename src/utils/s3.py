import boto3


class AWSUtil:

    def get_credentials(self):
        return self.session.get_credentials()

    def __init__(self):
        self.session = boto3.Session()
        self.s3 = boto3.resource('s3')
