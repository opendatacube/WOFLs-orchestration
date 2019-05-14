add-to-queue:
	BUCKET=frontiersi-odc-data \
	BUCKET_PATH=case-studies/usgs/LANDSAT_8 \
	LIMIT=9999 \
	AWS_DEFAULT_REGION=us-west-2 \
	QUEUE=landsat-to-wofs \
	python3 add_to_queue.py

up:
	docker-compose up

push:
	docker build ./Docker --tag crcsi/landsat-wofs
	docker push crcsi/landsat-wofs