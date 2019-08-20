#!/usr/bin/env python3                                                                                  

import logging

from datacube import Datacube

from datacube.index._api import Index
from datacube.index._datasets import DatasetResource

dc = Datacube()
ds =dc.index.datasets.get_datasets_for_location("s3://deafrica-data/usgs/c1/l8/201/54/2017/07/12/LC08_L\
1GT_201054_20170712_20170726_01_T2.xml")
#archive_location get_datasets_for_location                                                             
print (ds)
keybase = "s3://deafrica-data/"
with open("deadletters/frak_deadqueue.txt", 'r') as f:
    for line in f:
        key = keybase + line.rstrip()
        print (key)
        ds =dc.index.datasets.get_datasets_for_location(key)

        print (ds)
        for dataset in ds:
            results =dc.index.datasets.archive([dataset.id])

            #results =dc.index.datasets.archive_location(ds[0],key)                                     
            print (results)

