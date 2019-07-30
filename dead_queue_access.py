#!/usr/bin/env python3

import os
import sys
import boto3

QUEUE = os.environ.get('QUEUE', 'landsat-to-wofs')
DLQUEUE = os.environ.get('DLQUEUE', 'landsat-to-wofs-deadletter')

# Set up some AWS stuff

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=QUEUE)
dlqueue = sqs.get_queue_by_name(QueueName=DLQUEUE)


dlqueue.send_message(MessageBody="")



sys.exit()
messages = dlqueue.receive_messages(
        VisibilityTimeout=10,
        MaxNumberOfMessages=1
    )
message = messages[0]
print ('standard queue')
print (message)

dlqueue.send_message(MessageBody=message.body)

messages = dlqueue.receive_messages(
        VisibilityTimeout=10,
        MaxNumberOfMessages=1
    )
print ('dead queue')
message = messages[0]
print (message)
print('ran ok')
