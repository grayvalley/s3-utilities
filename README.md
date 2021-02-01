# s3-utilities
Utility tool for storing and downloading pandas DataFrames to and from AWS S3.

## Upload DataFrame
```python

# create a client
client = S3Client(aws_access_key_id, aws_secret_access_key)

# a random pandas dataframe
df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))

# upload gzipped dataframe
client.upload_dataframe(df, "gvt-test-bucket", "test-file")

```

## Download DataFrame
```python

# create a client
client = S3Client(aws_access_key_id, aws_secret_access_key)

# download gzipped dataframe
resp_df = client.download_dataframe("gvt-test-bucket", "test-file")

```

