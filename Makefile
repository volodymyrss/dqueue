REPO=odahub/dqueue
IMAGE=$(REPO):$(shell git describe --always)
CONTAINER=dqueue

listen: 
	gunicorn dqueueapp:app

run: build
	docker rm -f $(CONTAINER) || true
	docker run \
                -p 8100:8000 \
                -it \
	        --rm \
                --name $(CONTAINER) $(IMAGE)
	        #-e ODATESTS_BOT_PASSWORD=$(shell cat testbot-password.txt) \

build:
	rm -fv dist/*
	python setup.py sdist
	docker build -t $(IMAGE) .

push: build
	docker push $(IMAGE)
	docker tag $(IMAGE) $(REPO):latest
	docker push $(REPO):latest

test:
	mypy *.py
	#pylint -E  *.py
	python -m pytest  -sv

.FORCE:
