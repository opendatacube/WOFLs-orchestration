#!/bin/bash

set -e

export INPUT_FILE=$1
export JOB_NAME="wofs"
export IAM_ROLE="kubernetes-pipelines"
export IMAGE="opendatacube/pipelines:wofs-0.44"
export INPUT_S3_BUCKET="dea-public-data"
export OUTPUT_S3_BUCKET="dea-public-data-dev"
export OUTPUT_PATH="WOfS/WOFLs/v2.1.6/combined"
export LOG_LEVEL="INFO"
export FILE_PREFIX="S2_WATER_3577"
export DRY_RUN="False"

export CPU_REQUEST="400m"
export CPU_LIMIT="1000m"
export MEMORY_REQUEST="1Gi"
export MEMORY_LIMIT="16Gi"

export DB_DATABASE="ows"
export DB_PORT="5432"
export DB_HOSTNAME="devkube-dea-ga-gov-au-datakube-dev-mydb-rds.cxhoeczwhtar.ap-southeast-2.rds.amazonaws.com"

# Inject variables and run it
envsubst < job.yaml | kubectl apply -f -