#!/usr/bin/env python3
"""
Converts a Sentinel 2 Granule to a WOFS Observation

This script will take the location of a dataset yaml file, and output a WOFS Observation to an s3 bucket.
it requires a datacube with the Sentinel 2 Granule indexed.

Last Change: 2019/04/01
Authors: Belle Tissot & Tom Butler
"""

import datacube
import os
import sys
import logging
import boto3
import dateutil.parser
import xarray as xr
from wofs import classifier
from datacube import helpers
from datacube.utils import geometry
from datacube.model.utils import make_dataset
from ruamel.yaml import YAML
from pathlib import Path
import subprocess
from subprocess import check_call


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
    logging.debug(ds)
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
    filename = file_prefix + '_' + date + '_' + tile_id

    return filepath, filename


def _convert_to_cog(input_file, output_file):
    convert_args = ['rio',
                    'cogeo',
                    'create',
                    '--overview-resampling',
                    'nearest',
                    '--overview-blocksize',
                    '512',
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


def _create_metadata_file(dc, product_name, uri, extent, source, source_metadata):
    """
    Create a datacube metadata document

    :param Datacube dc: An initialised datacube
    :param str center_time: time of recording in iso_8601
    :param str uri: the path from metadata doc, to dataset files (just the filename)
    :param Dataset source: the source dataset
    :return: str metadata_doc: the contents of the metadata doc
    """
    # Get Time
    center_time = source_metadata['extent']['center_dt']

    # Find product
    product = dc.index.products.get_by_name(product_name)
    if product is None:
        logging.error('Could not find product %s in datacube', product_name)

    # Create a new dataset
    dts = make_dataset(product=product, sources=[source],
                       extent=extent, center_time=center_time, uri=uri)

    # tweak metadata
    metadata_doc = dts.metadata_doc
    metadata_doc['instrument'] = source_metadata['instrument']
    metadata_doc['platform'] = source_metadata['platform']
    metadata_doc['image']['bands']['water']['path'] = uri
    metadata_doc['grid_spatial']['projection']['valid_data'] = source_metadata['grid_spatial']['projection']['valid_data']

    logging.debug(metadata_doc)

    # Convert metadata to yaml
    with open('./ARD_METADATA.yaml', 'w') as f:
        yaml = YAML(typ='safe', pure=False)
        yaml.default_flow_style = False
        yaml.dump(metadata_doc, f)


def _upload(client, bucket, remote_path, local_file):

    data = open(local_file, 'rb')

    client.meta.client.upload_fileobj(
        Fileobj=data,
        Bucket=bucket,
        Key=remote_path
    )


def main(input_file):
    # Initialise clients
    s3 = boto3.resource('s3')
    dc = datacube.Datacube(app='WOFL-iron')
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=_get_log_level(LOG_LEVEL))

    # get contents of yaml file
    metadata = _read_yaml(s3, INPUT_S3_BUCKET, input_file)

    # Load data
    measurements = [
        'nbart_blue',
        'nbart_green',
        'nbart_red',
        'nbart_nir_1',
        'nbart_swir_2',
        'nbart_swir_3']

    data, extent, source = _load_data(dc, metadata['id'], measurements)
    formatted_data = _convert_to_numpy(data)

    # Load fmask
    fmask, fextent, fsource = _load_data(dc, metadata['id'], ['fmask'])

    # Classify it
    water = _classify(formatted_data)

    # Mask
    masked_data = _mask(water, fmask)

    # Check we have valid water data
    dtypes = {val.dtype for val in masked_data.data_vars.values()}

    if len(dtypes) is 1:
        # Get file naming config
        s3_filepath, filename = _generate_filepath(
            FILE_PREFIX,
            OUTPUT_PATH,
            metadata['extent']['center_dt'],
            metadata['tile_id'])

        masked_filename = filename + '_water.tiff'
        raw_filename = filename + 'raw.tiff'
        _save(masked_data, './' + raw_filename)
        _convert_to_cog(raw_filename, masked_filename)

        # Create metadata doc
        _create_metadata_file(
            dc,
            'wofs_albers',
            masked_filename,
            extent,
            source,
            metadata
        )

        # Upload data to S3
        _upload(s3,
                OUTPUT_S3_BUCKET,
                s3_filepath + masked_filename,
                local_file='./' + masked_filename)

        # Upload metadata to S3
        _upload(s3,
                OUTPUT_S3_BUCKET,
                s3_filepath + 'WATER_METADATA.yaml',
                local_file='./WATER_METADATA.yaml')

        logging.info('Done!')
    else:
        logging.error('Something went wrong during masking')


if __name__ == '__main__':
    main(INPUT_FILE)
