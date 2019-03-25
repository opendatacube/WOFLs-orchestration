import datacube
import os
import boto3
import wofs.wofs_app
from datacube import helpers
from datacube.utils import geometry
import xarray as xr
from ruamel.yaml import YAML


# Load config from env vars
INPUT_S3_BUCKET = os.environ['INPUT_S3_BUCKET']
INPUT_FILE = os.environ['INPUT_FILE']
OUTPUT_S3_BUCKET = os.environ['OUTPUT_S3_BUCKET']


def _read_yaml(client, bucket, path):
    """
    loads a YAML file from aws s3, returns a dict

    :param boto3.resource client: An initialised s3 client
    :param str bucket: The name of the s3 bucket
    :param str path: The filepath of the yaml file in the bucket
    :return: dict data
    """
    safety = 'safe'

    # Read the file from S3
    obj = client.Object(bucket, path).get(
        ResponseCacheControl='no-cache')
    raw = obj['Body'].read()

    # Convert the raw file to a dict
    yaml = YAML(typ=safety, pure=False)
    yaml.default_flow_style = False
    data = yaml.load(raw)

    return data


def _load_data(dc, ds_id):
    """
    loads data from a single dataset, in it's original crs

    :param Datacube dc: An initialised datacube client
    :param str ds_id: The id of the dataset we want to load
    :return: xarray data
    """

    # get the dataset and crs, so we don't change them on load
    d = dc.index.datasets.get(ds_id)
    crs = d.crs
    product = d.type.name
    # resample to highest band
    # res = '(-10,10)'
    res = d.measurements['fmask']['info']['geotransform'][5], d.measurements['fmask']['info']['geotransform'][1]

    data = dc.load(product=product,
                   datasets=[d],
                   output_crs=crs,
                   resolution=res)
    return data


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

    # Read the file from S3
    with xr.open_rasterio(path) as data:
        return data


def _classify(data):
    """
    performs WOFS classification on an xarray

    :param xarray data: An xarray of a single granule including bands: blue, green, red, nir, swir1, swir2
    :return: xarray water
    """
    print("Classify the dataset")
    water = wofs.classifier.classify(data).to_dataset(dim="water")
    print(water)
    water.attrs['crs'] = geometry.CRS(data.attrs['crs'])

    return water


def _save(ds, name):
    """
    saves xarray as a local file

    :param xarray ds: An xarray
    :param str name: the file path including filename
    """
    helpers.write_geotiff(name, ds)


def _mask(water, fmask):
    """
    masks a dataset using an fmask

    :param xarray water: The dataset to be masked
    :param xarray fmask: The fmask values
    :param str name: the file path including filename
    """
    # fmask: null(0), cloud(2), cloud shadow(3)
    return water.where(~(fmask.isin([0, 2, 3])))


if __name__ == '__main__':

    # Initialise clients
    s3 = boto3.resource('s3')
    dc = datacube.Datacube(app='dc-visualize')

    # get contents of yaml file
    metadata = _read_yaml(s3, INPUT_S3_BUCKET, INPUT_FILE)

    # Load data
    data = _load_data(dc, metadata.id)
    fmask = _load_fmask(s3, INPUT_S3_BUCKET, INPUT_FILE,
                        metadata.image.bands.fmask.path)

    # Classify it
    water = _classify(data)

    # Convert to COG
    _save(water, 'WATER.TIFF')
    _save(_mask(water, fmask), 'WATER.TIFF')

    # Upload to S3
