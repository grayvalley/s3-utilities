import pandas as pd
import numpy as np
import io

from s3utils import S3Client


def main():

    aws_access_key_id = 'AKIAIP7RQVTYSHJYOEBA'
    aws_secret_access_key = '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'

    client = S3Client(aws_access_key_id, aws_secret_access_key)

    # Upload DataFrame
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    client.upload_dataframe(df, "gvt-test-bucket", "test-file")

    # Download DataFrame
    resp_df = client.download_dataframe("gvt-test-bucket", "test-file")
    print(resp_df)

if __name__ == '__main__':
    main()