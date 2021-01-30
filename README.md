# s3-utilities


## Uploading JSON
```python

client = S3Client()

ata_dict = {"Hello": "World"}
client.upload_json(data_dict, "gvt-test", "json-hello-world")
```

## Downloading JSON
```python
resp_dict = client.download_json("gvt-test", "json-hello-world")
print("Downloaded:", resp_dict)
```

## Upload DataFrame
```python
df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
client.upload_dataframe(df, "gvt-test", "pickle-hello-world")
```
## Download DataFrame
```python
resp_df = client.download_dataframe("gvt-test", "pickle-hello-world")
print("Downloaded:", resp_df)
```