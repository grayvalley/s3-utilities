import os
import glob
import datetime
import posixpath
import logging
import logging.config
from optparse import OptionParser
from s3utils import S3Client

def init_logger():
    import yaml
    import time

    if not os.path.exists('log'):
        os.makedirs('log')

    with open('etc/logconf.yaml', 'r') as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)

    logging.config.dictConfig(config)
    logging.Formatter.converter = time.gmtime
    logger = logging.getLogger()
    return logger

def query_ready_files(source_dir, cutoff):
    pattern = os.path.join(source_dir, '*%*%*')
    files = glob.glob(pattern)
    lmod = lambda f : datetime.datetime.utcfromtimestamp(os.path.getmtime(f))
    return [f for f in files if lmod(f) < cutoff]

def get_bucket_key(filepath):
    filename = os.path.basename(filepath)
    key = filename.replace('%', posixpath.sep, 1) # 2021-02-10%XBTUSD%quote.pklgz -> 2021-02-10/XBTUSD%quote.pklg to generate folder in s3.
    return key

def upload_file(filepath, client, bucket, key):
    with open(filepath, 'rb') as f_hnd:
        logger.info(f'Uploading to (bucket={bucket}, key={key})...')
        return client.upload_fileobj(f_hnd, bucket=bucket, key=key)

def process_file(filepath, client, bucket, cleanup_source = False):
    logger.info(f'Preparing upload of {filepath}')
    try:
        key = get_bucket_key(filepath)
        if upload_file(filepath, client, bucket, key):
            logger.info('...success.')
            if cleanup_source:
                os.remove(filepath)
                logger.info('Local file deleted.')
            return True
        else:
            logger.error('...failed uploading file.')
            return False
    except Exception as exn:
        logger.exception(exn)
        return False

def run(source_dir, target_bucket, cleanup_source):
    # Upload files that have not been modified since 00:10 today.
    cutoff = datetime.datetime.combine(datetime.datetime.today(), datetime.time(minute=10))

    files = query_ready_files(source_dir, cutoff)
    client = S3Client.using_awsprofile(profile_name='s3')

    nuploads = 0
    for f in files:
        nuploads += 1*process_file(f, client, target_bucket, cleanup_source)
    logger.info(f'Uploaded {nuploads}/{len(files)} files.')
            

if __name__ == '__main__':
    logger = init_logger()
    logger.info('===== Start logging =====')

    parser = OptionParser()
    parser.add_option("--env", dest="environment", help="sets s3 bucket")
    (options, args) = parser.parse_args()
    if options.environment and options.environment.upper() == "PROD":
        source_dir = os.path.join(os.pardir, 'bitmex-s3-recorder', 'download')
        bucket = 'gvt-bitmex-l1'
        cleanup_source = True
    else:
        logger.warning('Running in test environment.')
        source_dir = os.path.join(os.pardir, 'bitmex-s3-recorder', 'download')
        bucket = 'gvt-test-bucket'
        cleanup_source = False

    try:
        run(source_dir, target_bucket=bucket, cleanup_source=cleanup_source)
    except Exception as exn:
        logger.exception(f'Unhandled exception: {exn}')

    logging.info('End of Program.')