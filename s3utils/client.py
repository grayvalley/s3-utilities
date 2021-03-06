import boto3
import gzip
import json
import io
import http

import pandas as pd

from botocore.exceptions import ClientError

import logging
logger = logging.getLogger(__name__)

class S3Client:

    def __init__(self, access_key_id, secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key)
        self._cli = self._session.client("s3")

    @classmethod
    def using_awsprofile(cls, profile_name):
        '''
        Alternative constructor using credentials stored in either credential or config file.
        ref: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        '''
        obj = cls.__new__(cls)
        obj._session = boto3.Session(profile_name=profile_name)
        obj._cli = obj._session.client("s3")
        return obj

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
            logger.error(e)
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
            logger.error(e)
            return None

        try:
            gz = gzip.GzipFile(fileobj=obj['Body'])
        except Exception as e:
            logger.error(e)
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
            logger.error(e)
            return False

        return True

    @staticmethod
    def _success_response(response, log_text):
        if not response:
            logger.error(f'Received empty {log_text} response.')
            return False
        
        status = http.HTTPStatus(response['ResponseMetadata']['HTTPStatusCode'])
        if status != http.HTTPStatus.OK:
            logger.error(f"{log_text.capitalize()} failed with HTTPStatus {status.description} and response \n {response}")
            return False
        else:
            return True

    def upload_fileobj(self, fileobj, bucket, key):
        """
        Upload file object (handle) to an S3 bucket
        :param bucket: Bucket to upload to
        :param key: unique key for the item

        :return: True if file was uploaded, else False

        """
        try:
            # Upload a new file
            response = self._cli.put_object(Bucket=bucket, Key=key, Body=fileobj)
            return self.__class__._success_response(response, 'upload')
        except Exception as exn:
            logger.exception(exn)
            return False

    def download_fileobj(self, bucket, key, stream_handle):
        """
        Download file object from a S3 bucket.

        :param bucket: bucket name
        :param key: key for the item
        :param stream_handle: binary file stream handle or buffer.

        :return: True if file was downloaded, else False
        """        
        #with open(filepath_or_buffer, 'wb+') as f_hnd:
        try:
            response = self._cli.get_object(Bucket=bucket, Key=key)
            if self.__class__._success_response(response, 'download'):
                for x in response['Body']:
                    stream_handle.write(x)
                return True
            else:
                return False
        except ClientError as e:
            logger.exception(e)
            return False
        except Exception as e:
            logger.exception(e)
            return False
        return True

    def download_fileobj2(self, bucket, key, stream_handle):
        """
        Download file object from a S3 bucket.

        :param bucket: bucket name
        :param key: key for the item
        :param filepath_or_buffer: full filepath for downloaded item

        :return: True if file was downloaded, else False
        """        
        #with open(filepath_or_buffer, 'wb+') as f_hnd:
        try:
            response = self._cli.get_object(Bucket=bucket, Key=key)
            if self.__class__._success_response(response, 'download'):
                for x in response['Body']:
                    stream_handle.write(x)
                return True
            else:
                return False
        except ClientError as e:
            logger.exception(e)
            return False
        except Exception as e:
            logger.exception(e)
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
