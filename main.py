import os
import glob
import datetime
import posixpath

import logging
import logging.config
#from logging.handlers import FileHandler

from s3utils import S3Client

def query_ready_files(source_dir, cutoff):
    # 1. Get files in folder, glob or listdir
    pattern = os.path.join(source_dir, '*%*%*')
    files = glob.glob(pattern)

    # 2. query files modified yday (GMT/UTC)
    lmod = lambda f : datetime.datetime.utcfromtimestamp(os.path.getmtime(f))
    ready_files = [f for f in files if lmod(f) < cutoff]
    return ready_files

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

def main():
    cutoff = datetime.datetime.combine(datetime.datetime.today(), datetime.time(minute=10))
    source_dir = os.path.join(os.pardir, 'bitmex-s3-recorder', 'download') # TODO have as cmd line argument, also consider moving IO folders outside repo.
    #source_dir = r'C:\Users\Pontus\Documents\test_upload'
    files = query_ready_files(source_dir, cutoff)
    client = S3Client.using_awsprofile(profile_name='s3')

    #bucket = 'gvt-test-bucket'
    bucket = 'gvt-bitmex-l1' # PROD
    for f in files:
        logging.info(f'Preparing upload of {f}')
        try:
            with open(f, 'rb') as f_hnd:
                key = os.path.basename(f)
                modkey = key.replace('%', posixpath.sep, 1) # 2021-02-10%XBTUSD%quote.pklgz -> 2021-02-10/XBTUSD%quote.pklg to generate folder in s3.
                logging.info(f'Uploading to (bucket={bucket}, key={modkey})...')
                success = client.upload_fileobj(f_hnd, bucket=bucket, key=modkey)
                if success:
                    logging.info('Success.')
                    # TODO: Delete file here...
        except Exception as exn:
            logging.exception(exn)
            continue
            

if __name__ == '__main__':
    logger = init_logger()
    logger.info('Start logging')
    try:
        main()
    except Exception as exn:
        logger.exception(f'Unhandled exception: {exn}')

    logging.info('DONE')







