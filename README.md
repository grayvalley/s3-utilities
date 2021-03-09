# s3-utilities
Utility tool for storing and downloading pandas DataFrames to and from AWS S3.


## Create Connection Client
```python
from s3utils import S3Client

# create a client by explicitly passing auth. details.
client = S3Client(aws_access_key_id, aws_secret_access_key)

# create client and using locally stored auth profile (recommended).
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
client = S3Client.using_awsprofile(profile_name='s3')
```

## Upload to S3
```python

# upload gzipped dataframe
df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
client.upload_dataframe(df, "gvt-test-bucket", "test-file")

# upload binary file or buffer
with open(filepath, 'rb') as f_hnd:
    success = client.upload_fileobj(f_hnd, bucket="gvt-test-bucket", key="test-bin-file")
```

## Download from S3
```python

# download gzipped dataframe
resp_df = client.download_dataframe("gvt-test-bucket", "test-file")

# download to binary file handle or byte buffer (io.BytesIO)
with open(filepath, 'wb+') as f_hnd:
    success = client.download_fileobj('gvt-test-bucket', 'test-bin-file', f_hnd)

```