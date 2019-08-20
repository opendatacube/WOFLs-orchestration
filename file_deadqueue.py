#!/usr/bin/env python3

import os
import boto3
import logging
import time

# Set us up some logging                                                       
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


#QUEUE = os.environ.get('QUEUE', 'dsg-test-queue')
QUEUE = os.environ.get('QUEUE', 'landsat-to-wofs')
QUEUE = os.environ.get('QUEUE', 'landsat-to-frak')
#DLQUEUE = os.environ.get('DLQUEUE', 'l2c-dead-letter')
DLQUEUE = os.environ.get('DLQUEUE', 'landsat-to-wofs-deadletter')
DLQUEUE = os.environ.get('DLQUEUE', 'landsat-to-wofs-errors')
DLQUEUE = os.environ.get('DLQUEUE', 'landsat-to-frak-deadletter')

# Set up some AWS stuff
s3 = boto3.client('s3')
s3r = boto3.resource('s3')
sqs = boto3.resource('sqs',  region_name='us-west-2')
queue = sqs.get_queue_by_name(QueueName=QUEUE)
dlqueue = sqs.get_queue_by_name(QueueName=DLQUEUE)

# dlqueue.send_message(MessageBody=message.body)
def getmessage():
    messages = dlqueue.receive_messages(
        VisibilityTimeout=60,
        MaxNumberOfMessages=1
    )
    if not messages:
        return None
    message = messages[0]
    #queue.send_message(MessageBody=message.body)
    logging.info("Message is {}.".format(message.body))
    #message.delete()
    time.sleep(.100)
    logging.info("Pushed a message to the file...")
    return message.body


def count_messages(a_queue):
    message_count = a_queue.attributes["ApproximateNumberOfMessages"]
    logging.info("There are {} messages on the queue.".format(message_count))
    return int(message_count)


def dead2file(filename):
    f = open(filename, "w+")
    n_messages = count_messages(dlqueue)
    while n_messages > 0:
        message = getmessage()
        if message is None:
            break
        f.write(message + '\n')
        n_messages = count_messages(dlqueue)

    f.close()
     

if __name__ == "__main__":
    filename = 'frak_deadqueue.txt'
    dead2file(filename)

