FROM opendatacube/datacube-core:latest

RUN mkdir -p /opt/data && \
    mkdir -p /opt/scripts
WORKDIR /opt/data

RUN apt-get update && apt-get install -y gfortran
RUN pip3 install git+https://github.com/GeoscienceAustralia/fc --no-deps --global-option=build --global-option='--executable=/usr/bin/env python3'

RUN pip3 install --index-url https://packages.dea.gadevs.ga/ wofs && \
    pip3 install ruamel.yaml rio-cogeo numexpr

# Define example env variables so users know how to use this image
ENV INPUT_S3_BUCKET="dea-public-data" \
    OUTPUT_S3_BUCKET="dea-public-data" \
    OUTPUT_PATH="WOfS/WOFLs/v2.1.6/combined" \
    LOG_LEVEL="INFO" \
    FILE_PREFIX="" \
    DB_HOSTNAME="localhost" \
    DB_PORT="5432" \
    DB_USERNAME="cube" \
    DB_PASSWORD="" \
    SQS_QUEUE_URL="wofs" \
    SQS_MESSAGE_PREFIX="/L2/sentinel-2-nrt/S2MSIARD/" \
    SQS_POLL_TIME_SEC=20 \
    JOB_MAX_TIME_SEC=300 \
    MAX_JOB_PER_WORKER=1 \
    MAKE_PUBLIC="True"

COPY watch-queue.py run.py run_frak.py /opt/scripts/
RUN chmod +x /opt/scripts/*.py && \
    ln -s /opt/scripts/watch-queue.py /usr/local/bin/watch-queue.py

CMD [ "watch-queue.py" ]
