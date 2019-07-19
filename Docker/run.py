#!/usr/bin/env python3
"""
Converts a Sentinel 2 Granule to a WOFS Observation

This script will take the location of a dataset yaml file, and output a WOFS Observation to an s3 bucket.
it requires a datacube with the Sentinel 2 Granule indexed.

Last Change: 2019/04/01
Authors: Belle Tissot & Tom Butler
"""

import logging
import os
import subprocess
import sys
import uuid
from distutils.util import strtobool
from pathlib import Path
from subprocess import check_call

import boto3
import datacube
import dateutil.parser
import numpy as np
import xarray as xr
from datacube import helpers
from datacube.model.utils import make_dataset
from datacube.storage import masking
from datacube.utils import geometry
from ruamel.yaml import YAML
from rio_cogeo.cogeo import cog_translate

from wofs import classifier

# This will be run in docker, so we load config from env vars
INPUT_S3_BUCKET = os.getenv('INPUT_S3_BUCKET',
                            'dea-public-data')
INPUT_FILE = os.getenv('INPUT_FILE',
                       'L2/sentinel-2-nrt/S2MSIARD/2019-03-20/S2A_OPER_MSI_ARD_TL_EPAE_20190320T024743_A019533_T52JEP_N02.07/ARD-METADATA.yaml')
OUTPUT_S3_BUCKET = os.getenv('OUTPUT_S3_BUCKET',
                             'dea-public-data')
OUTPUT_PATH = os.getenv('OUTPUT_PATH',
                        'WOfS/WOFLs/v2.1.6/combined')
LOG_LEVEL = os.getenv('LOG_LEVEL',
                      'INFO')
FILE_PREFIX = os.getenv('FILE_PREFIX',
                        '')
ROOT_FOLER =  os.getenv('ROOT_FOLER',
                        'usgs')

MAKE_PUBLIC = bool(strtobool(os.getenv('MAKE_PUBLIC', 'false').lower()))

INCLUDE_LINEAGE = bool(strtobool(os.getenv('INCLUDE_LINEAGE', 'false').lower()))

# COG profile
cog_profile = {
    'driver': 'GTiff',
    'interleave': 'pixel',
    'tiled': True,
    'blockxsize': 512,
    'blockysize': 512,
    'compress': 'DEFLATE',
    'predictor': 2,
    'zlevel': 9,
    'nodata': 1
}


def _get_s3_url(bucket_name, obj_key):
    return 's3://{bucket_name}/{obj_key}'.format(
        bucket_name=bucket_name, obj_key=obj_key)


def _get_log_level(level):
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


def _read_xml(dc, bucket, path):
    logging.info('Loading xml: {}'.format(_get_s3_url(bucket, path)))

    dataset_id = str(uuid.uuid5(uuid.NAMESPACE_URL, _get_s3_url(bucket, path)))

    logging.info("Getting dataset: {}".format(dataset_id))
    dataset = dc.index.datasets.get(dataset_id)

    return dataset.metadata_doc


def _read_yaml(client, bucket, path):
    """
    loads a YAML file from aws s3, returns a dict

    :param boto3.resource client: An initialised s3 client
    :param str bucket: The name of the s3 bucket
    :param str path: The filepath of the yaml file in the bucket
    :return: dict data
    """

    logging.info('Loading yaml: %s', 's3://' + bucket + '/' + path)
    # Read the file from S3
    obj = client.Object(bucket, path).get(
        ResponseCacheControl='no-cache')
    raw = obj['Body'].read()

    # Convert the raw file to a dict
    safety = 'safe'
    logging.debug('Using Yaml Safety: %s', safety)
    yaml = YAML(typ=safety, pure=False)
    yaml.default_flow_style = False
    data = yaml.load(raw)
    logging.debug('ID from yaml: %s', data['id'])
    logging.debug('fmask path from yaml: %s',
                  data['image']['bands']['fmask']['path'])

    return data


def _load_data(dc, ds_id, measurements):
    """
    loads data from a single dataset, in it's original crs

    :param Datacube dc: An initialised datacube client
    :param str ds_id: The id of the dataset we want to load
    :return: xarray data
    :return: str extent
    :return: dataset source
    """

    logging.info('Loading Dataset %s', ds_id)
    # get the dataset and crs, so we don't change them on load
    source = dc.index.datasets.get(ds_id)
    crs = source.crs
    product = source.type.name

    # resample to highest band
    res = (-30, 30)
    # res = d.measurements['fmask']['info']['geotransform'][5], d.measurements['fmask']['info']['geotransform'][1]

    logging.debug('Using CRS: %s', crs)
    logging.debug('Using resolution: %s', str(res))

    data = dc.load(product=product,
                   datasets=[source],
                   output_crs=crs,
                   resolution=res,
                   measurements=measurements)

    # Remove Time Dimension
    data = data.squeeze()

    extent = source.extent

    logging.info('Loaded Data: %s', data)
    return data, extent, source


def _convert_to_numpy(data):
    """
    Changes Sentinel 2 band names to Landsat, converts to numpy array

    :param xarray.Dataset data: A Sentinel 2
    :return: numpy array data: A 3D numpy array ordered in (bands,rows,columns), containing the spectral data.
    """
    return data.rename({
        'blue': 'blue',
        'green': 'green',
        'red': 'red',
        'nir': 'nir',
        'swir1': 'swir1',
        'swir2': 'swir2'
    }).to_array(dim='band')


def _classify(data):
    """
    performs WOFS classification on an xarray

    :param xarray data: An xarray of a single granule including bands: blue, green, red, nir, swir1, swir2
    :return: xarray water
    """
    logging.info('Classifying dataset')
    water = classifier.classify(data).to_dataset(dim="water")
    logging.info('Classification complete')
    logging.debug(water)
    water.attrs['crs'] = geometry.CRS(data.attrs['crs'])
    logging.info('Set CRS to: %s', data.attrs['crs'])

    return water


def _save(ds, name):
    """
    saves xarray as a local file

    :param xarray ds: An xarray
    :param str name: the file path including filename
    """
    logging.debug(ds)
    logging.info('Writing file: %s', name)
    helpers.write_geotiff(name, ds)


def _mask_landsat(water, pixel_qa):
    logging.info('Masking dataset')
    clean_pixel_mask = masking.make_mask(
        pixel_qa,
        cloud='no_cloud',
        cloud_shadow='no_cloud_shadow',
        nodata=False
    ).to_array()

    masked = water.where(clean_pixel_mask)
    return masked


def _mask(water, fmask):
    """
    masks a dataset using an fmask

    :param xarray water: The dataset to be masked
    :param xarray fmask: The fmask values
    :param str name: the file path including filename
    :return: xarray data: masked data
    """

    logging.info('Masking dataset')
    # fmask: null(0), cloud(2), cloud shadow(3)
    masked = water.where(~(fmask.fmask.isin([0, 2, 3])), 0)
    logging.debug('Masked Data')
    logging.debug(masked)
    return masked


def _generate_filepath(file_prefix, path_prefix, center_time, tile_id):
    """
    using the prefix, center_time, and tile_id, create /<prefix>/<yyyy-mm-dd>/wofs_tile_id/

    :param str prefix: A prefix to be applied to the filepath
    :param str center_time: time of recording in iso_8601
    :param str tile_id: The NRT Tile id
    :return: str filepath: the combined filepath
    """
    tile_id = tile_id.replace('_L1C_', '_WATER_', 1)
    tile_id = tile_id.replace('_ARD_', '_WATER_', 1)

    # pull the date from the center_time, set it as YYYY-MM-DD
    if not center_time[-1] is 'Z':
        logging.error(
            'center_time is in incorrect format, expected an iso_8601 but got: %s', center_time)
        sys.exit(1)
    date = dateutil.parser.parse(center_time).date().strftime("%Y-%m-%d")

    filepath = path_prefix + '/' + date + '/' + tile_id + '/'
    logging.info('Using remote filepath: %s', filepath)

    # Remove the '.' character from filename
    tile_id = tile_id.replace('.', '-', 1)
    filename = file_prefix + date + '_' + tile_id

    return filepath, filename


def _convert_to_cog(input_file, output_file):
    logging.info('converting to COG')
    convert_args = ['rio',
                    'cogeo',
                    'create',
                    '--overview-resampling',
                    'nearest',
                    '--overview-blocksize',
                    '256',
                    '--co',
                    'PREDICTOR=2',
                    '--co',
                    'ZLEVEL=9',
                    input_file,
                    output_file
                    ]
    try:
        check_call(convert_args, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, e.output))


def _create_metadata_file(dc, product_name, uri, extent, source, source_metadata, filename):
    """
    Create a datacube metadata document

    :param Datacube dc: An initialised datacube
    :param str center_time: time of recording in iso_8601
    :param str uri: the path from metadata doc, to dataset files (just the filename)
    :param Dataset source: the source dataset
    :return: str metadata_doc: the contents of the metadata doc
    """
    logging.info('Creating metadata file')
    # Get Time
    center_time = source_metadata['extent']['center_dt']

    # Find product
    product = dc.index.products.get_by_name(product_name)
    if product is None:
        logging.error('Could not find product %s in datacube', product_name)

    # Create a new dataset
    sources = []
    if INCLUDE_LINEAGE:
        sources = [source]
    dts = make_dataset(
        product=product, sources=sources,
        extent=extent, center_time=center_time, uri=uri
    )

    # tweak metadata
    metadata_doc = dts.metadata_doc
    metadata_doc['instrument'] = source_metadata['instrument']
    metadata_doc['platform'] = source_metadata['platform']
    metadata_doc['image']['bands']['water']['path'] = uri
    # metadata_doc['grid_spatial']['projection']['valid_data'] = source_metadata['grid_spatial']['projection']['valid_data']

    logging.debug(metadata_doc)

    # Convert metadata to yaml
    with open(filename, 'w') as f:
        yaml = YAML(typ='safe', pure=False)
        yaml.default_flow_style = False
        yaml.dump(metadata_doc, f)


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

def _pq_filter(pixel_qa):
    # From: https://github.com/bellemae/dea_bits/blob/master/TestWOfSbits.py
    NO_DATA = 1 << 0   # (dec 1)   bit 0: 1=pixel masked out due to NO_DATA in NBAR source, 0=valid data in NBAR
    MASKED_CLOUD = 1 << 6   # (dec 64)  bit 6: 1=pixel masked out due to cloud
    MASKED_CLOUD_SHADOW = 1 << 5   # (dec 32)  bit 5: 1=pixel masked out due to cloud shadow

    masked = np.zeros(pixel_qa.shape, dtype=np.uint8)
    masked[masking.make_mask(pixel_qa, nodata=True)] += NO_DATA
    masked[masking.make_mask(pixel_qa, cloud='cloud')] += MASKED_CLOUD
    masked[masking.make_mask(pixel_qa, cloud_shadow='cloud_shadow')] += MASKED_CLOUD_SHADOW

    return masked

def _mask_and_classify_landsat(data):
    # Create a mask, with bits set as per the WOFS data from GA
    mask = _pq_filter(data.pixel_qa)

    # Get the classified water out of the fancy magic decision tree
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
    wofs_premask = classifier.classify(data[bands].to_array(dim='band'))

    # It creates water where there's nodata, so mask that out
    nodata_mask = masking.make_mask(data.pixel_qa, nodata=False)
    wofs = wofs_premask.where(nodata_mask).astype(dtype=np.uint8)
    
    # Return the bitwise or to combine them
    wofl = wofs | mask
    wofl_dataset = wofl.to_dataset(name='wofl')
    wofl_dataset.attrs['crs'] = geometry.CRS(data.attrs['crs'])
    return wofl_dataset


def main(input_file):
    # Initialise clients
    s3 = boto3.resource('s3')
    dc = datacube.Datacube(app='WOFL-iron')
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=_get_log_level(LOG_LEVEL))

    # get contents of yaml file
    # metadata = _read_yaml(s3, INPUT_S3_BUCKET, input_file)
    metadata = _read_xml(dc, INPUT_S3_BUCKET, input_file)

    # Load data
    measurements = [
        'blue',
        'green',
        'red',
        'nir',
        'swir1',
        'swir2',
        'pixel_qa'
    ]

    data, extent, source = _load_data(dc, metadata['id'], measurements)
    masked_data = _mask_and_classify_landsat(data)

    # Check we have valid water data
    dtypes = {val.dtype for val in masked_data.data_vars.values()}
    logging.info("Created a new layer with {} data types.".format(dtypes))

    if len(dtypes) is 1:
        # Get file naming config
        # case-studies/usgs/LANDSAT_8/172/61/2013/06/20/LC08_L1TP_172061_20130620_20170503_01_T1.xml
        file_path = input_file.split(ROOT_FOLER + '/')[1].strip(".xml")
        # LANDSAT_8/172/61/2013/06/20/LC08_L1TP_172061_20130620_20170503_01_T1
        filename = file_path.split('/')[-1]

        s3_filepath = OUTPUT_PATH + '/' + file_path.split(filename)[0]
        filename = filename.replace('L1TP', 'WATER')

        masked_filename = filename + '_water.tiff'
        raw_filename = filename + '_raw.tiff'
        logging.info("Raw file: {}, COG file: {}, S3 path: {}".format(raw_filename, masked_filename, s3_filepath))
        _save(masked_data.squeeze().astype("uint8"), './' + raw_filename)
        cog_translate(
            raw_filename,
            masked_filename,
            cog_profile,
            overview_level=5,
            overview_resampling='nearest'
        )

        # Create metadata doc
        _create_metadata_file(
            dc,
            'ls8_usgs_wofs_scene',
            masked_filename,
            extent,
            source,
            metadata,
            './WATER_METADATA.yaml'
        )

        # Upload data to S3
        _upload(
            s3,
            OUTPUT_S3_BUCKET,
            s3_filepath + masked_filename,
            local_file='./' + masked_filename,
            makepublic=MAKE_PUBLIC,
            mimetype="image/tiff"
        )

        # Upload metadata to S3
        _upload(
            s3,
            OUTPUT_S3_BUCKET,
            s3_filepath + 'WATER_METADATA.yaml',
            local_file='./WATER_METADATA.yaml',
            makepublic=MAKE_PUBLIC,
            mimetype="application/x-yaml"
        )

        logging.info('Done!')
    else:
        logging.error('Something went wrong during masking')


if __name__ == '__main__':
    main(INPUT_FILE)
