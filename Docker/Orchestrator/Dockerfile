FROM opendatacube/wms:0.8.4 as wms

# dashboard deps
RUN apt-get update && apt-get install -y \
    python3-fiona python3-shapely \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /code/dashboard/product-summaries
COPY --from=opendatacube/dashboard:latest /code/ /code/dashboard/

RUN pip3 install -r requirements.txt  \
    && rm -rf $HOME/.cache/pip

RUN pip3 install schedule git+https://git@github.com/maxcountryman/flask-themes@master \
    && rm -rf $HOME/.cache/pip

RUN cd /code/dashboard/; python3 setup.py install

ADD orchestrate.py .
ADD https://raw.githubusercontent.com/opendatacube/datacube-dataset-config/master/scripts/index_from_s3_bucket.py .
