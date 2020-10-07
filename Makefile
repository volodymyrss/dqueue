REPO=odahub/dqueue
IMAGE=$(REPO):$(shell git describe --always)
CONTAINER=dqueue

prep:
	pylint -E dqueue tests/*py || echo "linted" && \
	mypy dqueue || echo "mypyed" && \
	python -m pytest tests --maxfail=1

listen: 
	gunicorn --workers 1 dqueue.api:app -b 0.0.0.0:8000 --log-level DEBUG

run: build
	docker rm -f $(CONTAINER) || true
	docker run \
          -p 8100:8000 \
          -it \
		      -e API_BASE=/ \
	        --rm \
                --name $(CONTAINER) $(IMAGE)
	        #-e ODATESTS_BOT_PASSWORD=$(shell cat testbot-password.txt) \

run-guardian: build
	docker rm -f $(CONTAINER) || true
	docker run \
          -p 8100:8000 \
          -it \
		      -e API_BASE=/ \
			  	-e APP_MODE=guardian \
					-e ODAHUB="http://in.internal.odahub.io/staging-1-3/dqueue@queue-osa11" \
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
