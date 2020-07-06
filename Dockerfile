FROM python:3.8

ADD requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt

#ADD workflow-schema.json /workflow-schema.json
ADD templates /templates
ADD static /static
ADD dist/* /dist/
RUN pip install /dist/*

ENV API_BASE=/staging-1-3/dqueue

ENTRYPOINT gunicorn --workers 8 dqueue.api:app -b 0.0.0.0:8000 --log-level DEBUG
