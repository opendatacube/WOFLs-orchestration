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
#DLQUEUE = os.environ.get('DLQUEUE', 'l2c-dead-letter')
DLQUEUE = os.environ.get('DLQUEUE', 'landsat-to-wofs-deadletter')

# Set up some AWS stuff
s3 = boto3.client('s3')
s3r = boto3.resource('s3')
sqs = boto3.resource('sqs',  region_name='us-west-2')
queue = sqs.get_queue_by_name(QueueName=QUEUE)
dlqueue = sqs.get_queue_by_name(QueueName=DLQUEUE)

# dlqueue.send_message(MessageBody=message.body)
def dead2living():
    messages = dlqueue.receive_messages(
        VisibilityTimeout=10,
        MaxNumberOfMessages=1
    )
    if not messages:
        return
    message = messages[0]
    queue.send_message(MessageBody=message.body)
    logging.info("Message is {}.".format(message.body))
    message.delete()
    time.sleep(.300)
    logging.info("Pushed a message to the living...")
        
def count_messages(a_queue):
    message_count = a_queue.attributes["ApproximateNumberOfMessages"]
    logging.info("There are {} messages on the queue.".format(message_count))
    return int(message_count)

if __name__ == "__main__":
    n_messages = count_messages(dlqueue)
    while n_messages > 0:
        dead2living()

        n_messages = count_messages(dlqueue)



