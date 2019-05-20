#!/usr/bin/env python3


import logging
import os
import uuid
import warnings
from datetime import date
from distutils.util import strtobool

import datacube
import yaml
from datacube.helpers import write_geotiff
from datacube.model import Measurement
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from fc.fractional_cover import fractional_cover

INPUT_S3_BUCKET = os.getenv('INPUT_S3_BUCKET',
                            'dea-public-data')

INPUT_FILE = os.getenv('INPUT_FILE',
                       'file:///home/david/Downloads/sr/LC08_L1TP_074072_20160917_20170321_01_T1.yaml')
OUTPUT_S3_BUCKET = os.getenv('OUTPUT_S3_BUCKET',
                             'dea-public-data')

#SOURCE_FILE = 'file:///home/david/Downloads/sr/LC08_L1TP_074072_20160917_20170321_01_T1.yaml'

dc = datacube.Datacube(app='fc')

MAKE_PUBLIC = bool(strtobool(os.getenv('MAKE_PUBLIC', 'false').lower()))

bands_of_interest = ['green', 'red', 'nir', 'swir1', 'swir2']

sensor_regression_coefficients = {
    'blue': [0.00041, 0.97470],
    'green': [0.00289, 0.99779],
    'red': [0.00274, 1.00446],
    'nir': [0.00004, 0.98906],
    'swir1': [0.00256, 0.99467],
    'swir2': [-0.00327, 1.02551]
}

fc_measurements = [
    Measurement(name='BS', units='percent', dtype='int16', nodata=-1),
    Measurement(name='PV', units='percent', dtype='int16', nodata=-1),
    Measurement(name='NPV', units='percent', dtype='int16', nodata=-1),
    Measurement(name='UE', units='1', dtype='int16', nodata=-1)]


def _get_s3_url(bucket_name, obj_key):
    return 's3://{bucket_name}/{obj_key}'.format(
        bucket_name=bucket_name, obj_key=obj_key)


def _read_xml(dc, bucket, path):
    logging.info('Loading xml: {}'.format(_get_s3_url(bucket, path)))

    dataset_id = str(uuid.uuid5(uuid.NAMESPACE_URL, _get_s3_url(bucket, path)))

    logging.info("Getting dataset: {}".format(dataset_id))
    dataset = dc.index.datasets.get(dataset_id)

    return dataset.metadata_doc


def _upload(client, bucket, remote_path, local_file, makepublic=False, mimetype=None):
    logging.info("Uploading file {} to bucket and path {}/{}".format(
        local_file,
        bucket,
        remote_path
    ))
    data = open(local_file, 'rb')

    extra_args = dict()

    if makepublic:
        extra_args['ACL'] = 'public-read'
    if mimetype is not None:
        extra_args['ContentType'] = mimetype

    args = { 'ExtraArgs': extra_args }

    client.meta.client.upload_fileobj(
        Fileobj=data,
        Bucket=bucket,
        Key=remote_path,
        **args
    )


def create_cog(source, output, bidx):
    cogeo_profile = 'deflate'
    nodata = -1
    overview_level = 6
    overview_resampling = 'nearest'
    threads = 8
    output_profile = cog_profiles.get(cogeo_profile)
    output_profile.update(dict(BIGTIFF=os.environ.get("BIGTIFF", "IF_SAFER")))
    block_size = min(
        int(output_profile["blockxsize"]), int(output_profile["blockysize"])
    )

    config = dict(
        NUM_THREADS=threads,
        GDAL_TIFF_INTERNAL_MASK=os.environ.get("GDAL_TIFF_INTERNAL_MASK", True),
        GDAL_TIFF_OVR_BLOCKSIZE=os.environ.get("GDAL_TIFF_OVR_BLOCKSIZE", block_size),
    )

    cog_translate(
        src_path=source,
        dst_path=output,
        dst_kwargs=output_profile,
        indexes=bidx,
        nodata=nodata,
        web_optimized=False,
        add_mask=False,
        overview_level=overview_level,
        overview_resampling=overview_resampling,
        config=config,
        quiet=False
    )


def load_and_generate_fc(query):
    sr = dc.load(measurements=bands_of_interest,
                 group_by='solar_day',
                 **query).squeeze()
    if not sr:
        return None

    warnings.filterwarnings('ignore')
    fc = fractional_cover(sr, fc_measurements, sensor_regression_coefficients)
    warnings.filterwarnings('always')

    del sr

    return fc


def write_fc_band(fc, key, filename):
    slim_dataset = fc[[key]]  # create a one band dataset
    attrs = slim_dataset[key].attrs.copy()  # To get nodata in
    del attrs['crs']  # It's format is poor
    del attrs['units']  # It's format is poor
    slim_dataset[key] = fc.data_vars[key].astype('int16', copy=True)
    output_filename = filename + '_' + key + '_TEMP' + '.tif'
    write_geotiff(output_filename, slim_dataset, profile_override=attrs)
    return output_filename


def main(source_filename):
    source_metadata = _read_xml(dc, INPUT_S3_BUCKET, source_filename)
    source = dc.index.datasets.get(source_metadata['id'])
    logging.info("Source: {}".format(source))
    #sources = dc.index.datasets.get_datasets_for_location(source_filename, 'exact')
    #source = next(sources)

    crs = 'EPSG:' + str(source['crs']['epsg'])

    query = {
        'datasets': [source['id']],
        'crs': crs,
        'resolution': (-30, 30),
        'output_crs': crs,
        'product': source.type.name
    }

    uri = source.uris[0]
    filename = uri[uri.rfind('/') + 1:uri.rfind('.yaml')] + '_FC'

    fc = load_and_generate_fc(query)
    if fc:
        # keep track of different FC Measurement files
        output_files = {}

        # for each FC mesurement, output to TIFF and COG
        for measurement in fc_measurements:
            key = measurement['name']
            uncogged_output_file = write_fc_band(fc, key, filename)
            target_filename = filename + '_' + key + '.tif'

            # as we have created this bands separately, band index (bidx) is always 0
            bidx = 0

            create_cog(uncogged_output_file, target_filename, bidx)
            os.remove(uncogged_output_file)
            output_files[key] = target_filename

        # Generate new YAML
        new_doc = source.metadata_doc_without_lineage()
        new_doc['id'] = str(uuid.uuid4())
        new_doc['creation_dt'] = date.today()
        new_doc['image'] = {'bands': {}}
        new_doc['product_type'] = 'fractional_cover'
        del new_doc['processing_level']

        for fc_measurement in fc_measurements:
            key = fc_measurement['name']
            new_doc['image']['bands'][key] = {'path': output_files[key]}
        yaml_filename = filename + '.yaml'
        with open(yaml_filename, 'w') as outfile:
            yaml.dump(new_doc, outfile, default_flow_style=False)

        # Upload each band file
        for fc_measurement in fc_measurements:
            key = fc_measurement['name']
            # Upload data to S3
            _upload(
                s3,
                OUTPUT_S3_BUCKET,
                s3_filepath + output_files[key],
                local_file=output_files[key],
                makepublic=MAKE_PUBLIC,
                mimetype="image/tiff"
            )

        # Upload metadata to S3
        _upload(
            s3,
            OUTPUT_S3_BUCKET,
            s3_filepath + yaml_filename,
            local_file=yaml_filename,
            makepublic=MAKE_PUBLIC,
            mimetype="application/x-yaml"
        )

        logging.info('Done!')
    else:
        logging.error('No FC Found')


if __name__ == '__main__':
    main(INPUT_FILE)
