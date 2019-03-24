import boto3
import click
import json
import schedule
from time import sleep
from hashlib import md5
from yaml import load
from functools import partial
from datetime import date, datetime, timedelta
from pathlib import PurePath


from index_from_s3_bucket import add_dataset, get_s3_url
import datacube
from datacube import Datacube
from datacube.ui.click import pass_config, environment_option, config_option
from datacube_wms.product_ranges import add_product_range

from cubedash.generate import cli
import os


import logging
_LOG = logging.getLogger("datakube-orchestration")
_LOG.setLevel(os.getenv("ORCHESTRATION_LOG_LEVEL", "INFO"))

sqs = boto3.client('sqs')
s3 = boto3.resource('s3')

SQS_LONG_POLL_TIME_SECS = 20
DEFAULT_POLL_TIME_SECS = 60
DEFAULT_SOURCES_POLICY="verify"
MAX_MESSAGES_BEFORE_EXTENT_CALCULATION = 10

def update_cubedash(product_names):
    click_ctx = click.get_current_context()
    # As we are invoking a cli command, intercept the call to exit
    try:
        click_ctx.invoke(cli, product_names=product_names)
    except SystemExit:
        pass


def archive_datasets(product, days, dc, enable_cubedash=False):
    def get_ids(datasets):
        for d in datasets:
            ds = index.datasets.get(d.id, include_sources=True)
            for source in ds.sources.values():
                yield source.id
            yield d.id

    index = dc.index
    past = datetime.now() - timedelta(days=days)
    query = datacube.api.query.Query(product=product, time=[date(1970, 1, 1), past])
    datasets = index.datasets.search_eager(**query.search_terms)
    if len(datasets) > 0:
        _LOG.info("Archiving datasets: %s", [d.id for d in datasets])
        index.datasets.archive(get_ids(datasets))
        add_product_range(dc, product)
        if enable_cubedash:
            update_cubedash([product.name])


def process_message(index, message, prefix, sources_policy=DEFAULT_SOURCES_POLICY):
    # message body is a string, need to parse out json a few times
    inner = json.loads(message)
    s3_message = json.loads(inner["Message"])
    errors = dict()
    datasets = []
    skipped = 0
    if "Records" not in s3_message:
        errors["no_record"] = "Message did not contain S3 records"
        return datasets, errors

    for record in s3_message["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        if prefix is None or len(prefix) is 0 or any([PurePath(key).match(p) for p in prefix]):
            try:
                errors[key] = None
                obj = s3.Object(bucket_name, key).get(ResponseCacheControl='no-cache')
                data = load(obj['Body'].read())
                # NRT data may not have a creation_dt, attempt insert if missing
                if "creation_dt" not in data:
                    try:
                        data["creation_dt"] = data["extent"]["center_dt"]
                    except KeyError:
                        pass
                uri = get_s3_url(bucket_name, key)
                dataset, errors[key] = add_dataset(data, uri, index, sources_policy)
                if errors[key] is None:
                    datasets.append(dataset)
            except Exception as e:
                errors[key] = e
        else: 
            _LOG.debug("Skipped: %s as it does not match prefix filters", key)
            skipped = skipped + 1
    return datasets, skipped, errors

def delete_message(sqs, queue_url, message):
    receipt_handle = message["ReceiptHandle"]
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle)
    _LOG.debug("Deleted Message %s", message.get("MessageId"))


def query_queue(sqs, queue_url, dc, prefix, poll_time=DEFAULT_POLL_TIME_SECS,
                sources_policy=DEFAULT_SOURCES_POLICY, enable_cubedash=False):

    index = dc.index
    messages_processed = 0
    products_to_update = []

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=SQS_LONG_POLL_TIME_SECS)

        if "Messages" not in response:
            if messages_processed > 0:
                _LOG.info("Processed: %d messages", messages_processed)
                messages_processed = 0
                for p in products_to_update:
                    add_product_range(dc, p)
                if enable_cubedash:
                    update_cubedash([p.name for p in products_to_update])
            return
        else:
            for message in response.get("Messages"):
                message_id = message.get("MessageId")
                body = message.get("Body")
                md5_of_body = message.get("MD5OfBody", "")
                md5_hash = md5()
                md5_hash.update(body.encode("utf-8"))
                # Process message if MD5 matches
                if (md5_of_body == md5_hash.hexdigest()):
                    _LOG.info("Processing message: %s", message_id)
                    messages_processed += 1
                    datasets, skipped, errors = process_message(index, body, prefix, sources_policy)
                    for d in datasets:
                        product = d.type
                        if product not in products_to_update:
                            products_to_update.append(product)
                    if not any(errors.values()):
                        _LOG.info("Successfully processed %d datasets in %s, %d datasets were skipped",
                                  len(datasets), message.get("MessageId"), skipped)
                    else:
                        # Do not delete message
                        for key, error in errors.items():
                            _LOG.error("%s had error: %s", key, error)
                else:
                    _LOG.warning("%s MD5 hashes did not match, discarding message: %s", message_id, body)
                delete_message(sqs, queue_url, message)


@click.command(help="Python script to continuously poll SQS queue that is specified")
@environment_option
@config_option
@pass_config
@click.option("--queue",
    "-q",
    default=None)
@click.option("--poll-time",
    default=DEFAULT_POLL_TIME_SECS)
@click.option('--sources_policy',
    default=DEFAULT_SOURCES_POLICY,
    help="verify, ensure, skip")
@click.option("--prefix",
    default=None,
    multiple=True)
@click.option("--archive",
    default=None,
    multiple=True,
    type=(str, int))
@click.option("--archive-check-time",
    default="01:00")
@click.option("--cubedash",
    is_flag=True,
    default=False)
def main(config,
         queue,
         poll_time,
         sources_policy,
         prefix,
         archive,
         archive_check_time,
         cubedash):
    dc = Datacube(config=config)

    if queue is not None:
        sqs = boto3.client('sqs')
        response = sqs.get_queue_url(QueueName=queue)
        queue_url = response.get('QueueUrl')
        query = partial(
            query_queue,
            sqs,
            queue_url,
            dc,
            prefix,
            poll_time=poll_time,
            sources_policy=sources_policy,
            enable_cubedash=cubedash)

        schedule.every(poll_time).seconds.do(query)

    for product, days in archive:
        do_archive = partial(
            archive_datasets,
            product,
            days,
            dc,
            enable_cubedash=cubedash)
        do_archive()
        schedule.every().day.at(archive_check_time).do(do_archive)

    while True:
        schedule.run_pending()
        sleep(1)

if __name__ == "__main__":
    main()
