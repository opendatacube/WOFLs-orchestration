#!/bin/bash

set -e

export INPUT_FILE=$1
export JOB_NAME="WOFS"
export IAM_ROLE="kubernetes-pipeline"
export IMAGE="opendatacube/pipeline:wofs-0.1"
export INPUT_S3_BUCKET="dea-public-data"
export OUTPUT_S3_BUCKET="dea-public-data"
export OUTPUT_PATH="WOfS/WOFLs/v2.1.6/combined/"
export LOG_LEVEL="DEBUG"
export FILE_PREFIX="S2_WATER_3577"
export DRY_RUN="False"

# Inject variables and run it
envsubst < job.yaml | kubectl apply -f -