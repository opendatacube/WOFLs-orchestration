#!/usr/bin/env python3

import os
import logging
import boto3

# Set us up some logging
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

# Set some env vars
BUCKET = os.environ.get('BUCKET', 'deafrica-data')
PATH = os.environ.get('BUCKET_PATH', 'usgs')
QUEUE = os.environ.get('QUEUE', 'landsat-to-wofs')

LIMIT = int(os.environ.get('LIMIT', 10))

# Get an S3 client
s3 = boto3.client('s3')

# Get the queue
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=QUEUE)


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                yield key
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_items(limit=10, suffix=''):
    count = 0
    logging.info("Adding {} items from: {}/{} to the queue {}".format(limit, BUCKET, PATH, QUEUE))
    items = get_matching_s3_keys(BUCKET, PATH, suffix=suffix)
    for item in items:
        count += 1
        if count >= limit:
            break

        if count % 100 == 0:
            logging.info("Pushed {} items...".format(count))

        # Create a big list of items we're processing.
        queue.send_message(MessageBody=item)


if __name__ == "__main__":
    get_items(limit=LIMIT, suffix='.xml')
