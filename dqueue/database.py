import os
import peewee # type: ignore
import logging
import datetime

# beware that insert task may fail if mysql field is too small!

logger = logging.getLogger(__name__)

from playhouse.db_url import connect # type: ignore
from playhouse.shortcuts import model_to_dict, dict_to_model # type: ignore

# use http://docs.peewee-orm.com/projects/flask-peewee/en/latest/index.html
def connect_db():
    try:
        db = connect(os.environ.get("DQUEUE_DATABASE_URL","mysql+pool://root@localhost/dqueue?max_connections=42&stale_timeout=8001.2"))
        logger.info(f"successfully connected to db: {db}")

        return db

    except Exception as e:
        logger.warning("unable to connect to DB: %s", repr(e))

db = connect_db()


class CallbackQueue(peewee.Model):
    database = None

    url = peewee.TextField()
    params_json =  peewee.TextField()
    timestamp = peewee.DateTimeField(default=datetime.datetime.now)
    state = peewee.CharField() # new, processing, complete
    returned_status_json = peewee.TextField()

    class Meta:
        database = db


class TaskWorkerKnowledge(peewee.Model):
    database = None

    key = peewee.CharField()
    worker_knowledge_hash =  peewee.CharField()
    score = peewee.FloatField()
    timestamp = peewee.DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = db

class TaskEntry(peewee.Model):
    database = None

    queue = peewee.CharField(default="default")

    key = peewee.CharField(primary_key=True)
    state = peewee.CharField()
    worker_id = peewee.CharField()

    task_dict_string = peewee.TextField()

    created = peewee.DateTimeField()
    modified = peewee.DateTimeField()

    update_expected_in_s = peewee.FloatField(default=-1)

    class Meta:
        database = db


class EventLog(peewee.Model):
    queue = peewee.CharField(default="default")

    task_key = peewee.CharField(default="unset")
    task_state = peewee.CharField(default="unset")

    worker_id = peewee.CharField()
    worker_state = peewee.CharField(default="unset")

    timestamp = peewee.DateTimeField(default=datetime.datetime.now)
    message = peewee.CharField(default="unset")
    
    spent_s = peewee.FloatField(default=0)

    class Meta:
        database = db

try:
    db.create_tables([TaskEntry, EventLog, TaskWorkerKnowledge, CallbackQueue])
    has_mysql = True
except peewee.OperationalError:
    has_mysql = False
except Exception:
    has_mysql = False
