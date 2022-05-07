image_name = 'pyspark-example:dev'

build:
	docker build -t ${image_name} .

run:
	docker run ${image_name} driver local:///opt/application/main.py