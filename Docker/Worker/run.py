#!/usr/bin/env python3
"""
Converts a Sentinel 2 Granule to a WOFS Observation

This script will take the location of a dataset yaml file, and output a WOFS Observation to an s3 bucket.
it requires a datacube with the Sentinel 2 Granule indexed.

Last Change: 2019/03/27
Authors: Belle Tissot & Tom Butler
"""

import yaml as pyyaml
import datacube
import os
import sys
import boto3
import dateutil.parser
from wofs import classifier
from datacube import helpers
from datacube.utils import geometry
from datacube.model.utils import make_dataset
import xarray as xr
from ruamel.yaml import YAML
import logging
from datacube.utils.geometry import GeoBox
from affine import Affine
from pathlib import Path

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper

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
                        'S2_WATER_3577')
DRY_RUN = os.getenv('DRY_RUN',
                    None)


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


def _load_data(dc, ds_id):
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
    measurements = [
        'nbart_blue',
        'nbart_green',
        'nbart_red',
        'nbart_nir_1',
        'nbart_swir_2',
        'nbart_swir_3']

    # resample to highest band
    res = (-10, 10)
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

    logging.debug('Loaded Data: %s', data)
    return data, extent, source


def _convert_to_numpy(data):
    """
    Changes Sentinel 2 band names to Landsat, converts to numpy array

    :param xarray.Dataset data: A Sentinel 2 
    :return: numpy array data: A 3D numpy array ordered in (bands,rows,columns), containing the spectral data.
    """
    return data.rename({
        'nbart_blue': 'blue',
        'nbart_green': 'green',
        'nbart_red': 'red',
        'nbart_nir_1': 'nir',
        'nbart_swir_2': 'swir1',
        'nbart_swir_3': 'swir2'
    }).to_array(dim='band')


def _load_fmask(client, bucket, metadata_path, fmask_path):
    """
    loads fmask from s3

    :param boto3.resource client: An initialised s3 client
    :param str bucket: The name of the s3 bucket
    :param str metadata_path: The filepath of the yaml file in the bucket
    :param str fmask_path: The relative path of fmask from the yaml file
    :return: xarray data
    """
    # create a path to the fmask file
    basepath, filename = os.path.split(metadata_path)
    path = 's3://' + bucket + '/' + basepath + '/' + fmask_path

    logging.info('Loading fmask from: %s', path)

    # Read the file from S3
    with xr.open_rasterio(path) as data:
        return data


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
    logging.debug('Set CRS to: %s', data.attrs['crs'])

    return water


def _save(ds, name):
    """
    saves xarray as a local file

    :param xarray ds: An xarray
    :param str name: the file path including filename
    """
    logging.info('Writing file: %s', name)
    helpers.write_geotiff(name, ds)


def _mask(water, fmask):
    """
    masks a dataset using an fmask

    :param xarray water: The dataset to be masked
    :param xarray fmask: The fmask values
    :param str name: the file path including filename
    :return: xarray data: masked data
    """
    logging.debug('Masking dataset')
    # fmask: null(0), cloud(2), cloud shadow(3)
    return water.where(~(fmask.isin([0, 2, 3])))


def _generate_filepath(file_prefix, path_prefix, center_time, tile_id):
    """
    using the prefix, center_time, and tile_id, create /<prefix>/<yyyy-mm-dd>/<x>/<y>/

    :param str prefix: A prefix to be applied to the filepath
    :param str center_time: time of recording in iso_8601
    :param str tile_id: The NRT Tile id, must end in _AXXXXX_TXXXXX_XXX.XX
    :return: str filepath: the combined filepath
    """
    # pull the date from the center_time, set it as YYYY-MM-DD
    if not center_time[-1] is 'Z':
        logging.error(
            'center_time is in incorrect format, expected an iso_8601 but got: %s', center_time)
        sys.exit(1)
    date = dateutil.parser.parse(center_time).date().strftime("%Y-%m-%d")

    # we pull the military coords from the yaml path
    s = tile_id.split('_')

    # AXXXXX
    x = s[-3]
    if not x.startswith('A') or not len(x) is 7:
        logging.error(
            'Cannot determine military coords, expected AXXXXXXX but got: %s', x)
        sys.exit(1)

    # TXXXXX
    y = s[-2]
    if not y.startswith('T') or not len(y) is 6:
        logging.error(
            'Cannot determine military coords, expected TXXXXXX but got: %s', y)
        sys.exit(1)

    filepath = path_prefix + '/' + date + '/' + x + '/' + y + '/'
    logging.info('Using remote filepath: %s', filepath)

    filename = file_prefix + '_' + center_time + '_' + x + '_' + y

    return filepath, filename


def _create_metadata_file(dc, product_name, center_time, uri, extent, source):
    """
    Create a datacube metadata document

    :param Datacube dc: An initialised datacube
    :param str center_time: time of recording in iso_8601
    :param str uri: the path from metadata doc, to dataset files (just the filename)
    :param Dataset source: the source dataset
    :return: str metadata_doc: the contents of the metadata doc
    """
    # from https://github.com/GeoscienceAustralia/wofs-confidence/blob/master/confidence/wofs_filtered.py
    # Compute metadata

    product = dc.index.products.get_by_name(product_name)

    dts = make_dataset(product=product, sources=[source],
                       extent=extent, center_time=center_time, uri=uri)
    metadata = pyyaml.dump(
        dts.metadata_doc, Dumper=SafeDumper, encoding='utf-8')
    return metadata


def _upload(client, bucket, remote_path, local_file=None, data=None):

    if local_file is not None:
        data = open(local_file, 'rb')

    client.put_object(
        Bucket=bucket,
        Key=remote_path,
        Body=data
    )


if __name__ == '__main__':

    # Initialise clients
    s3 = boto3.resource('s3')
    dc = datacube.Datacube(app='WOFL-iron')
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=_get_log_level(LOG_LEVEL))

    # get contents of yaml file
    metadata = _read_yaml(s3, INPUT_S3_BUCKET, INPUT_FILE)

    # Load data
    data, extent, source = _load_data(dc, metadata['id'])
    formatted_data = _convert_to_numpy(data)
    fmask = _load_fmask(s3, INPUT_S3_BUCKET, INPUT_FILE,
                        metadata['image']['bands']['fmask']['path'])

    # Classify it
    water = _classify(formatted_data)

    # Get file naming config
    filename, s3_filepath = _generate_filepath(
        FILE_PREFIX,
        OUTPUT_PATH,
        metadata['extent']['center_dt'],
        metadata['tile_id'])

    masked_filename = filename + '_water.tiff'

    metadata_doc = _create_metadata_file(
        dc,
        'wofs_nrt',
        metadata['extent']['center_dt'],
        masked_filename,
        extent,
        source
    )

    # Save to local system as COG
    _save(water, './' + filename + '_raw_water.tiff')
    _save(_mask(water, fmask), './' + masked_filename)

    # Upload data to S3
    _upload(s3,
            OUTPUT_S3_BUCKET,
            s3_filepath + '/' + masked_filename,
            local_file='./' + masked_filename)

    # Upload metadata to S3
    _upload(s3,
            OUTPUT_S3_BUCKET,
            s3_filepath + '/ARD_METADATA.yaml',
            data=metadata_doc)

    logging.info('Done!')
