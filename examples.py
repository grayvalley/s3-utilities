import pandas as pd
import numpy as np
import io

from s3utils import S3Client


def main():

    aws_access_key_id = ''
    aws_secret_access_key = '+H+Ev8i+nBiOGJ8MgA9vbcByCJjfh'

    client = S3Client(aws_access_key_id, aws_secret_access_key)

    # Upload json
    data_dict = {"Hello": "World"}
    client.upload_json(data_dict, "gvt-test", "json-hello-world")

    # Download json
    resp_dict = client.download_json("gvt-test", "json-hello-world")
    print("Downloaded:", resp_dict)

    # Upload DataFrame
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    client.upload_dataframe(df, "gvt-test", "pickle-hello-world")

    # Download DataFrame
    resp_df = client.download_dataframe("gvt-test", "pickle-hello-world")
    print("Downloaded:", resp_df)

if __name__ == '__main__':
    main()