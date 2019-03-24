import water_classifier_and_wofs
import DEADataHandling
import numpy as np
import os
import sys
import warnings
import xarray as xr
import matplotlib as mpl

# modules for datacube
import datacube
from datacube.storage import masking
from datacube.helpers import write_geotiff

# set datacube alias (just a string with what you're doing)
dc = datacube.Datacube(app='dc-WOfS and water classifier')

# Import external functions from dea-notebooks
sys.path.append('../10_Scripts/')

# ignore datacube warnings (needs to be last import statement)
warnings.filterwarnings('ignore', module='datacube')

# Use this to manually define an upper left/lower right coords
lat_max = -12.4
lat_min = -12.7
lon_max = 135.2
lon_min = 134.9

# define temporal range
start_of_epoch = '2016-01-01'
end_of_epoch = '2016-05-01'

# define Landsat sensors of interest
sensors = ['ls8', 'ls7', 'ls5']

#Query is created
query = {'time': (start_of_epoch, end_of_epoch), }
query['x'] = (lon_min, lon_max)
query['y'] = (lat_max, lat_min)
query['crs'] = 'EPSG:4326'

print(query)

mask_dict = {'cloud_acca': 'no_cloud',
             'cloud_fmask': 'no_cloud',
             'cloud_shadow_acca': 'no_cloud_shadow',
             'cloud_shadow_fmask': 'no_cloud_shadow',
             'blue_saturated': False,
             'green_saturated': False,
             'red_saturated': False,
             'nir_saturated': False,
             'swir1_saturated': False,
             'swir2_saturated': False}

# using the load function from DEADataHandling to get the data and filter
nbart = DEADataHandling.load_clearlandsat(dc, query,
                                          product='nbart',
                                          masked_prop=0,
                                          mask_dict=mask_dict)

# Use water clasifier function
warnings.filterwarnings('ignore')  # turn off warnings
water_class = water_classifier_and_wofs.water_classifier(nbart)
warnings.filterwarnings('always')  # turn on warnings
print(water_class)

# note, this is using only one band for the count, and this isn't robust.
total_water_obs = water_class.wofs.sum(dim='time')
nbar_count = nbart.blue.count(dim='time')
wofs = ((total_water_obs / nbar_count)*100)


# Convert to a dataset and restore spatial attributes
dataset = wofs.to_dataset(name='wofs')
dataset.attrs['affine'] = nbart.affine
dataset.attrs['crs'] = nbart.crs

write_geotiff('wofs_{}_{}.tif'.format(start_of_epoch, end_of_epoch), dataset)

# Save to s3
