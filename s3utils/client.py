import logging
import boto3
from botocore.exceptions import ClientError


class S3Client:

    def __init__(self):
        self._cli = boto3.client('s3')

    def upload_file(self, file_name, bucket, object_name=None):
        """

        Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used

        :return: True if file was uploaded, else False
        """
        if not isinstance(file_name, str):
            raise TypeError('file_name')

        if not isinstance(bucket, str):
            raise TypeError('bucket')

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            response = self._cli.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False

        return True

    def download_file(self, bucket, object_name, file_name):
        """

        Download a file from an S3 bucket

        :param file_name: File to download
        :param bucket: Bucket to download from
        :param object_name: S3 object name. If not specified then file_name is used

        :return: downloaded data
        """

        if not isinstance(bucket, str):
            raise TypeError('bucket')

        if not isinstance(object_name, str):
            raise TypeError('object_name')

        if not isinstance(file_name, str):
            raise TypeError('file_name')

        return self._cli.download_file(bucket, object_name, file_name)