import datacube
import sys
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
# strip the filename from the path
OUTPUT_S3_BUCKET = os.environ['OUTPUT_S3_BUCKET']


def _read_yaml(client, bucket, path):

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


def _classify(data):
    print("Classify the dataset")
    water = wofs.classifier.classify(data).to_dataset(dim="water")
    print(water)
    water.attrs['crs'] = geometry.CRS(data.attrs['crs'])

    return water


def _save(ds, name):
    helpers.write_geotiff(name, ds)


def _mask(water, fmask):
    # fmask: null(0), cloud(2), cloud shadow(3)
    return water.where(~(fmask.isin([0, 2, 3])))


if __name__ == '__main__':

    # Initialise clients
    s3 = boto3.resource('s3')
    dc = datacube.Datacube(app='dc-visualize')

    # get id from yaml file
    metadata = _read_yaml(s3, INPUT_S3_BUCKET, INPUT_FILE)

    # Load data
    data = _load_data(dc, metadata.id)

    # Classify it
    water = _classify(data)

    # Convert to COG
    _save(water, loc+outfile)
    _save(_mask(water, ds[-1]), 'WATER.TIFF')

    # Upload to S3

    year = "2019"
    monthdays = ['0214']  # , '0219',]
    for md in monthdays:
        loc = "/g/data/u46/users/bt2744/work/data/floodFeb19/s2_imagery/2019-" + \
            md[0:2]+"-"+md[2:4]+"/"

        cells = ["T54KWG", "T54KXG", "T54KXF",
                 "T54KXE", "T54KXD", "T54KXC"]  # "T54KWC"]

        for cell in cells:
            infile = cell+"-"+md+".vrt"
            outfile = "water_"+infile.split('.')[0]+".tif"

            print("Processing "+infile+"....")
            ds = _load_file_data(loc+infile)
            water = _classify(ds[0:6])
            _save(water, loc+outfile)
            _save(_mask(water, ds[-1]), loc+"m_"+outfile)
