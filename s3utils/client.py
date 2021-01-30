import logging
import pickle
import boto3
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
        return data

    def upload_dataframe(self, dataframe, bucket, key):
        """
        Upload pickle buffer to an S3 bucket

        :param dataframe: the dataframe containing our data
        :param bucket: Bucket to upload to
        :param key: unique key for the item

        :return: True if file was uploaded, else False

        """
        buffer = pickle.dumps(dataframe)

        try:
            self._cli.put_object(Bucket=bucket, Body=buffer, Key=key)
            return True
        except Exception as e:
            logging.error(e)
            return False

    def download_dataframe(self, bucket, key):
        resp = self._cli.get_object(Bucket=bucket, Key=key)
        return pickle.loads(resp['Body'].read())

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

    def list_items(self, bucket_name):
        """
        List item names in a bucket.

        :param bucket_name: name of the bucket we are interested in

        :return: list of item keys (not folders).
        """
        def is_folder(item):
            return '/' in item["Key"]
        items = []
        for content in self._cli.list_objects(Bucket=bucket_name)['Contents']:
            if not is_folder(content):
                items.append(content['Key'])
        return items

    def list_folders(self, bucket_name):
        """
        List folder names in a bucket.

        :param bucket_name: name of the bucket we are interested in

        :return: list of folder names
        """
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

        :param bucket_name: name of the bucket in which we want to create the wolder.
        :param folder_name: name of the folder we want to create

        """
        self._cli.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
