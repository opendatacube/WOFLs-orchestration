FROM opendatacube/datacube-core:latest

RUN pip3 install cligj==0.5.0


RUN pip3 install --index-url https://packages.dea.gadevs.ga/ wofs && \
    pip3 install ruamel.yaml rio-cogeo

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
    SQS_POLL_TIME_SEC=60 \
    JOB_MAX_TIME_SEC=300 \
    MAX_JOB_PER_WORKER=1 \
    MAKE_PUBLIC="true"

COPY watch-queue.py watch-queue.py
COPY run.py run.py

CMD [ "python3", "watch-queue.py" ]
