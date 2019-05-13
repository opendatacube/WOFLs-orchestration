add-to-queue:
	BUCKET=frontiersi-odc-data \
	BUCKET_PATH=case-studies/usgs/LANDSAT_8 \
	LIMIT=10 \
	AWS_DEFAULT_REGION=us-west-2 \
	QUEUE=landsat-to-wofs \
	python3 add_to_queue.py

up:
	docker-compose up
