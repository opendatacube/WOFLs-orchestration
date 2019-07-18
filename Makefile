add-to-queue:
	BUCKET=deafrica-data \
	BUCKET_PATH=test/victoria/LANDSAT_8 \
	LIMIT=9999 \
	AWS_DEFAULT_REGION=us-west-2 \
	QUEUE=landsat-to-wofs \
	python3 add_to_queue.py

add-to-frak:
	BUCKET=frontiersi-odc-data \
	BUCKET_PATH=case-studies/usgs/LANDSAT_8 \
	LIMIT=9999 \
	AWS_DEFAULT_REGION=us-west-2 \
	QUEUE=landsat-to-frak \
	python3 add_to_queue.py

up-wofs:
	docker-compose up \
		-e TYPE=wofs \
		-e SQS_QUEUE_URL=landsat-to-wofs \
		-e OUTPUT_PATH=usgs/wofs \
	 	-e FILE_PREFIX=L8_WATER_3577

up-frak:
	docker-compose run \
		-e TYPE=frak \
		-e SQS_QUEUE_URL=landsat-to-frak \
		-e OUTPUT_PATH=usgs/frak \
		-e FILE_PREFIX=L8_FRAK_3577 \
		wofl-copter


push:
	docker build ./Docker --tag crcsi/landsat-wofs
	docker push crcsi/landsat-wofs
