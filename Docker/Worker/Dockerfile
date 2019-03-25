FROM opendatacube/datacube-core:latest

RUN pip3 --index-url https://packages.dea.gadevs.ga/ wofs

ENV INPUT_S3_BUCKET="dea-public-data" \
    INPUT_FILE="/L2/sentinel-2-nrt/S2MSIARD/2019-03-20/S2A_OPER_MSI_ARD_TL_EPAE_20190320T024743_A019533_T52JEP_N02.07/ARD-METADATA.yaml" \
    OUTPUT_S3_BUCKET="dea-public-data"

COPY run.py run.py

ENTRYPOINT [ "python3", "run.py" ]