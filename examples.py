
from s3utils import S3Client


def main():

    aws_access_key_id = 'AKIAI5MZJGI7V3Q4L7RA'
    aws_secret_access_key = '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'

    client = S3Client(aws_access_key_id, aws_secret_access_key)

    # Upload json
    data_dict = {"Hello": "World"}
    client.upload_json(data_dict, "gvt-test", "json-hello-world")

    # Download json
    resp_dict = client.download_json("gvt-test", "json-hello-world")
    print("Downloaded:", resp_dict)

if __name__ == '__main__':
    main()