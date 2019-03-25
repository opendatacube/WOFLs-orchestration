FROM opendatacube/datacube-core:latest

# Install C version of PyYaml
RUN apt-get update && apt-get install -y --no-install-recommends \
    libyaml-dev \
    && rm -rf /var/lib/apt/lists/* && \
    pip3 --no-cache-dir install --verbose --force-reinstall -I pyyaml

RUN pip3 --index-url https://packages.dea.gadevs.ga/ wofs

ENV INPUT_S3_BUCKET=dea-public-data \
    INPUT_FILE= "/L2/sentinel-2-nrt/S2MSIARD/2019-03-20/S2A_OPER_MSI_ARD_TL_EPAE_20190320T024743_A019533_T52JEP_N02.07/ARD-METADATA.yaml" \
    OUTPUT_S3_BUCKET= dea-public-data

COPY run.py run.py

ENTRYPOINT [ "python3", "run.py" ]