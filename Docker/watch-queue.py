#!/usr/bin/env python3

import boto3
import json
import run
import os
import logging
import time
from hashlib import md5
from datetime import date, datetime, timedelta
from pathlib import PurePath


SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', 'landsat-to-wofs')
SQS_MESSAGE_PREFIX = os.getenv('SQS_MESSAGE_PREFIX', '')
SQS_POLL_TIME_SEC = os.getenv('SQS_POLL_TIME_SEC', '10')
JOB_MAX_TIME_SEC = os.getenv('JOB_MAX_TIME_SEC', '20')
MAX_JOB_PER_WORKER = os.getenv('MAX_JOB_PER_WORKER', '1')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


def _getlogging_level(level):
    """
    converts text to log level

    :param str level: a string with the value of: NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL
    """
    return {
        'NOTSET': logging.NOTSET,
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }.get(level, logging.INFO)


def delete_message(sqs, queue_url, message):
    """
    deletes a message from the queue to ensure it isn't processed again

    :param boto3.client sqs: an initialised boto sqs client
    :param str queue_url: the sqs queue we're deleting from
    :param str queue_url: the sqs queue we're deleting from
    """
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message["ReceiptHandle"])
    logging.debug("Deleted Message %s", message.get("MessageId"))


def processing_loop(sqs, sqs_queue_url, sqs_message_prefix, sqs_poll_time, job_max_time, max_jobs):

    messages_processed = 0
    more_mesages = True
    while more_mesages:
        # Check the queue for messages
        logging.debug("Checking Queue, %s wait time: %s, job time: %s, max jobs per worker, %s",
                      sqs_queue_url, sqs_poll_time, job_max_time, max_jobs)
        start_time = time.time()
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            WaitTimeSeconds=sqs_poll_time,
            VisibilityTimeout=job_max_time,
            MaxNumberOfMessages=1)
        if "Messages" not in response:
            # No messages, exit successfully
            logging.info("No new messages, exiting successfully")
            more_mesages = False
        else:
            for message in response.get("Messages"):
                message_id = message.get("MessageId")
                logging.info("Processing message: %s", message_id)

                # Validate message contents
                body = message.get("Body")
                md5_of_body = message.get("MD5OfBody", "")
                md5_hash = md5()
                md5_hash.update(body.encode("utf-8"))
                if (md5_of_body == md5_hash.hexdigest()):
                    # Read message
                    logging.info(body)
                    key = body
                    run.main(key)
                    delete_message(
                        sqs, sqs_queue_url, message
                    )
                    job_time = time.time() - start_time
                    if job_time > job_max_time:
                        logging.error(
                            'Exceeded max job time, job may be processed multiple time')
                        logging.debug("Processing took %s, of a maximum %s", str(
                            job_time), job_max_time)
                        messages_processed += 1
                else:
                    # Didn't validate, delete the message
                    logging.warning(
                        "%s MD5 hashes did not match, discarding message: %s", message_id, body)
                    delete_message(sqs, sqs_queue_url, message)


if __name__ == '__main__':

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=_getlogging_level(LOG_LEVEL))
    sqs = boto3.client('sqs')

    response = sqs.get_queue_url(QueueName=SQS_QUEUE_URL)
    queue_url = response.get('QueueUrl')

    processing_loop(sqs,
                    queue_url,
                    [SQS_MESSAGE_PREFIX],
                    int(SQS_POLL_TIME_SEC),
                    int(JOB_MAX_TIME_SEC),
                    int(MAX_JOB_PER_WORKER))
