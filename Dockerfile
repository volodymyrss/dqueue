FROM python:3.8

ADD requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt

#ADD workflow-schema.json /workflow-schema.json
ADD templates /templates
ADD static /static
ADD dist/* /dist/
RUN pip install /dist/*


ENTRYPOINT gunicorn dqueue.api:app -b 0.0.0.0:8000 --log-level INFO
