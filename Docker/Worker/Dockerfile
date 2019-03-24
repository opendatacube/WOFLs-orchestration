FROM opendatacube/datacube-core:latest

ENV AWS_S3_BUCKET=dea-public-data

COPY run.py run.py

ENTRYPOINT [ "python3", "run.py" ]