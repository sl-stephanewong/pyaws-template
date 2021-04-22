import boto3


class AWSUtil:

    def get_credentials(self):
        return self._session.get_credentials()

    def get_resource_s3(self):
        return self._s3

    def get_all_buckets(self) -> list[str]:
        buckets = []
        for bucket in self._s3.buckets.all():
            buckets.append(bucket.name)
        return buckets

    def get_objects_from_bucket(self, bucket_name: str) -> list[str]:
        files = []
        for file in self._s3.Bucket(bucket_name).objects.all():
            files.append(file.key)
        return files

    def __init__(self):
        self._session = boto3.Session()
        self._s3 = boto3.resource('s3')
