import logging
import boto3
import gzip
import json
import io

import pandas as pd

from botocore.exceptions import ClientError


class S3Client:

    def __init__(self, access_key_id, secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key)
        self._cli = self._session.client("s3")

    def upload_json(self, data_dict, bucket, key):
        """
        Upload dictionary as JSON to an S3 bucket

        :param data_dict: the dictionary containing our data
        :param bucket: Bucket to upload to
        :param key: unique key for the item

        :return: True if file was uploaded, else False

        """
        try:
            self._cli.put_object(Bucket=bucket, Body=json.dumps(data_dict), Key=key)
            return True
        except Exception as e:
            logging.error(e)
            return False

    def download_json(self, bucket, key):

        resp = self._cli.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read().decode()
        return json.loads(data)

    def download_dataframe(self, bucket, key, index_col=0, header=0):
        """
        Download gzip compressed pandas.DataFrame from a S3 bucket.

        :param bucket: bucket name
        :param key: key for the item
        :param index_col: index column location
        :param header: header row location

        :return: pandas.DataFrame if success, None if failure
        """

        try:
            obj = self._cli.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(e)
            return None

        try:
            gz = gzip.GzipFile(fileobj=obj['Body'])
        except Exception as e:
            logging.error(e)
            return None

        # load stream directly to DF
        return pd.read_csv(gz, index_col=index_col, header=header, dtype=str)

    def upload_dataframe(self, df, bucket, key):
        """
        Upload pandas.Dataframe to a S3 bucket

        :param df: the dataframe
        :param bucket: bucket name
        :param key: key for the dataframe

        :return: True if file was uploaded, else False
        """

        try:
            # write DF to string stream
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # reset stream position
            csv_buffer.seek(0)
            # create binary stream
            gz_buffer = io.BytesIO()

            # compress string stream using gzip
            with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))

            # write stream to S3
            obj = self._cli.put_object(Bucket=bucket, Key=key, Body=gz_buffer.getvalue())

        except ClientError as e:
            logging.error(e)
            return False

        return True

    def list_items(self, bucket_name):
        """
        List item names in a bucket.

        :param bucket_name: name of the bucket

        :return: list of item keys.
        """
        response = self._cli.list_objects(Bucket=bucket_name)

        if 'Contents' not in response:
            return []

        def is_folder(item):
            return '/' in item["Key"]

        items = []
        for content in response['Contents']:
            if not is_folder(content):
                items.append(content['Key'])
        return items

    def list_folders(self, bucket_name):
        """
        List folder names in a bucket.

        :param bucket_name: name of the bucket we are interested in

        :return: list of folder names
        """
        response = self._cli.list_objects(Bucket=bucket_name)

        if 'Contents' not in response:
            return []

        def is_folder(item):
            return '/' in item["Key"]

        folders = []
        for content in self._cli.list_objects(Bucket=bucket_name)['Contents']:
            if is_folder(content):
                folders.append(content['Key'])
        return folders

    def create_folder(self, bucket_name, folder_name):
        """
        Create a folder into S3 bucket.

        :param bucket_name: bucket name.
        :param folder_name: folder name

        """
        self._cli.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
