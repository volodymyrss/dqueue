import traceback
import datetime
import os
import time
import socket
from hashlib import sha224
from collections import defaultdict
import glob
import logging
import io
import urllib.parse
import traceback

import dqueue.core 
import dqueue.app
import dqueue.tools as tools

import peewee # type: ignore
import json

from flask import Flask
from flask import render_template,make_response,request,jsonify
from flasgger import Swagger, SwaggerView, Schema, fields # type: ignore

decoded_entries={} # type: ignore

db = dqueue.core.db

app = dqueue.app.app


template = {
  "swagger": "2.0",
#  "info": {
 #   "title": "My API",
#    "description": "API for my data",
    #"contact": {
    #  "responsibleOrganization": "ME",
    #  "responsibleDeveloper": "Me",
    #  "email": "me@me.com",
    #  "url": "www.me.com",
    #},
    #"termsOfService": "http://me.com/terms",
    #"version": "0.0.1"
 # },
  #"host": "mysite.com",  # overrides localhost:500
  "basePath": os.environ.get("API_BASE", "/"),  # base bash for blueprint registration
  "schemes": [
    "http",
    "https"
  ],
 # "operationId": "getmyData"
}


swagger = Swagger(app, template=template)

print("setting up app", app, id(app))

logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger(__name__)


## === schemas

class TaskData(Schema):
    pass

class Task(Schema):
    state = fields.Str()
    queue = fields.Str()
    task_key = fields.Str()
    task_data = TaskData

class TaskList(Schema):
    tasks = fields.Nested(Task, many=True)

class LogEntry(Schema):
    message = fields.Str()

class QueueLogEntry(Schema):
    state = fields.Str()
    task_key = fields.Str()

class ViewLog(Schema):
    event_log = fields.Nested(LogEntry, many=True)

class QueueList(Schema):
    queues = fields.Nested(fields.Str(), many=True)

class Status(Schema):
    status = fields.Str()

## === views

class TaskListView(SwaggerView):
    operationId = "listTasks"
    parameters = [
        {
            "name": "queue",
            "in": "query",
            "type": "string",
            "required": False,
        },
        {
            "name": "state",
            "in": "query",
            "type": "string",
            "enum": ["submitted", "waiting", "done", "all"],
            "required": False,
            "default": "all",
        }
    ]
    responses = {
        200: {
            "description": "A list of tasks",
            "schema": TaskList
        }
    }

    def get(self, state="all"):
        """
        get list of tasks
        """

        queue = dqueue.core.Queue(request.args.get('queue', 'default'))

        tasks = [e for e in tools.list_tasks(include_task_data=True)]

        #tasks = queue.list_tasks(decode=True) #state=state)

        logger.debug("list tasks returns %s, tasks", tasks)

        return jsonify(
                tasks=tasks
            )

app.add_url_rule(
         '/tasks',
          view_func=TaskListView.as_view('api_tasks'),
          methods=['GET']
)

class WorkerOffer(SwaggerView):
    operationId = "getOffer"

    parameters = [
                {
                    'name': 'worker_id',
                    'in': 'path',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'task data',
                    'schema': Task,
                },
            204: {
                    'description': 'problem: no tasks can be offered',
                }
        }

    def get(self, worker_id):
        queue = dqueue.core.Queue(request.args.get('queue', 'default'), worker_id=worker_id)

        try:
            task = queue.get()
            logger.warning("got task: %s", task)
            return jsonify(
                    task.as_dict,
                )
        except dqueue.Empty:
            r = jsonify(
                    problem="no entries"
                )

            r.status_code = 204
            return r


app.add_url_rule(
          '/worker/<string:worker_id>/offer',
          view_func=WorkerOffer.as_view('worker_offer_task'),
          methods=['GET']
)

class WorkerAnswer(SwaggerView):
    operationId = "answer"

    parameters = [
                {
                    'name': 'worker_id',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'task_dict',
                    'in': 'body',
                    'required': True,
                    'schema': Task, 
                },
            ]

    responses = {
            200: {
                    'description': 'its ok',
                    'schema': Task,
                 }
            }

    def post(self):
        queue = dqueue.core.Queue(request.args.get('queue', 'default'))

        task_dict = request.json

        logger.debug("setting current task in %s to %s", queue, task_dict)

        queue.state = "done"
        queue.current_task = dqueue.core.Task.from_task_dict(task_dict)
        queue.current_task_stored_key = queue.current_task.key
        task = queue.current_task

        queue.task_done()

        return jsonify(
                    { 'task_key': task.key, **task.as_dict}
               )


app.add_url_rule(
         '/worker/answer',
          view_func=WorkerAnswer.as_view('worker_answer_task'),
          methods=['POST']
)


class TryAllLocked(SwaggerView):
    operationId = "try_all_locked"

    # locally or remotely?

    parameters = [
                {
                    'name': 'worker_id',
                    'in': 'path',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'unlocked!',
                },
            204: {
                    'description': 'problem: no tasks can be offered',
                }
        }

    def get(self, worker_id):
        queue = dqueue.core.Queue(request.args.get('queue', 'default'), worker_id=worker_id)

        r = queue.try_all_locked()

        logger.info("unlocked: %d", len(r))

        return jsonify(tasks=r)


app.add_url_rule(
          '/tasks/<string:worker_id>/try_all_locked',
          view_func=TryAllLocked.as_view('tasks_try_all_locked'),
          methods=['GET']
)

class TaskViewLog(SwaggerView):
    operationId = "view_log"

    parameters = [
                {
                    'name': 'task_key',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'since',
                    'in': 'query',
                    'required': False,
                    'type': 'number',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'task log dict',
                    'schema': ViewLog,
                }
        }

    def get(self):
        task_key = request.args.get('task_key', None)
        if task_key == "":
            task_key = None

        since = request.args.get('since', 0, type=int)

        queue = dqueue.core.Queue()

        r = queue.view_log(task_key=task_key, since=since)

        logger.info("view_log api returns %d entris", len(r))
        #for e in r:
        #    logger.info("view_log api returns entry: %s", e)

        return jsonify(
                    event_log=r,
                )

app.add_url_rule(
     '/task/view_log',
      view_func=TaskViewLog.as_view('task_view_log'),
      methods=['GET']
)

class TaskLogView(SwaggerView):
    operationId = "logTask"

    parameters = [
                {
                    'name': 'message',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'state',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'worker_id',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'task_key',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'task dict',
                }
        }

    def post(self):
        message = request.args.get('message', 'no message')
        queue = request.args.get('queue', 'default')
        task_key = request.args.get('task_key')
        worker_id = request.args.get('worker_id')
        state = request.args.get('state')

        queue = dqueue.core.Queue(worker_id=worker_id, queue=queue)

        logger.info("log_task worker_id %s task_key %s message %s", worker_id, task_key, message)

        queue.log_task(message=message, task_key=task_key, state=state)

        logger.debug("task log: %s", queue.view_log())
        assert len(queue.view_log()) > 0

        return jsonify(
                    message
                )

app.add_url_rule(
     '/worker/log',
      view_func=TaskLogView.as_view('worker_log'),
      methods=['POST']
)

class QueueLogView(SwaggerView):
    operationId = "logQueue"

    parameters = [
                {
                    'name': 'message',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'spent_s',
                    'in': 'query',
                    'required': True,
                    'type': 'number',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
                {
                    'name': 'worker_id',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'task dict',
                }
        }

    def post(self):
        message = request.args.get('message')
        spent_s = request.args.get('spent_s', type=float)
        worker_id = request.args.get('worker_id')
        queue = request.args.get('queue', 'default')

        queue = dqueue.core.Queue(worker_id=worker_id, queue=queue)

        logger.info("log_queue worker_id %s message %s spent %s", worker_id, message, spent_s)

        logger.info("log queue returns: %s", queue.log_queue(message, spent_s, worker_id=worker_id))

        return jsonify(
                    message
                )

app.add_url_rule(
     '/worker/queuelog',
      view_func=QueueLogView.as_view('worker_queue_log'),
      methods=['POST']
)

class WorkerQuestion(SwaggerView):
    operationId = "questionTask"

    parameters = [
                {
                    'name': 'worker_id',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'task_data',
                    'in': 'body',
                    'required': True,
                    'schema': TaskData,
                },
                {
                    'name': 'token',
                    'in': 'query',
                    'required': True,
                    'type': 'string',
                },
                {
                    'name': 'queue',
                    'in': 'query',
                    'required': False,
                    'type': 'string',
                },
            ]

    responses = {
            200: {
                    'description': 'task dict',
                    'schema': Task
                }
        }

    def post(self):
        queue = request.args.get('queue', 'default')
        worker_id = request.args.get('worker_id')
        task_data = request.json

        queue = dqueue.core.Queue(worker_id=worker_id, queue=queue)

        print("got:", worker_id, task_data)

        task_entry = queue.put(task_data)

        logger.warning("questioned task: %s", task_entry)
        return jsonify(
                    task_entry
                )

app.add_url_rule(
     '/worker/question',
      view_func=WorkerQuestion.as_view('worker_question_task'),
      methods=['POST']
)

class TaskInfoView(SwaggerView):
    operationId = "task_info"

    parameters = [
                {
                    'name': 'task_key',
                    'in': 'path',
                    'required': True,
                    'type': 'string',
                }
            ]

    responses = {
            200: {
                    'description': 'task data',
                    'schema': Task,
                }
        }

    def get(self, task_key):
        queue = dqueue.core.Queue()

        task_dict = queue.task_by_key(task_key)
        logger.warning("requested task_key %s %s", task_key, task_dict)

        if task_dict is None:
            return jsonify()

        return jsonify(
                **task_dict
            )

app.add_url_rule(
         '/task/view/<task_key>',
          view_func=TaskInfoView.as_view('view_task_info'),
          methods=['GET']
)

@app.route("/tasks/resubmit/<string:scope>/<string:selector>")
def tasks_resubmit(scope, selector):
    """
    ---
    operationId: 'resubmit'
    parameters:
    - name: 'queue'
      in: 'query'
      required: false
      type: 'string'

    - name: 'scope'
      in: 'path'
      enum: ['state', 'task']
      required: true 
      type: 'string'

    - name: 'selector'
      in: 'path'
      required: True
      type: 'string'

    responses:
        200: 
            description: 'entries purged'
    """

    #queue = dqueue.core.Queue(queue)
    n = tools.resubmit(scope, selector)
    return jsonify(
            nentries=n
        )

@app.route("/queues/list")
def list_queues():
    """
    ---
    operationId: 'list'

    definitions:
        QueueList:
            type: 'array'
            items: 
                type: 'string'

    responses:
        200: 
            description: 'queue list'
            schema:
                $ref: '#/definitions/QueueList'
    """

    
    queue = dqueue.core.Queue()

    ql = queue.list_queues()

    print("queues on the server:", ql)
    r = [q.queue for q in ql]
    print("r:", r)

    return jsonify(
            r
        )

@app.route("/tasks/purge")
def tasks_purge():
    """
    ---
    operationId: 'purge'
    parameters:
    - name: 'state'
      in: 'query'
      required: false
      type: 'string'
    - name: 'queue'
      in: 'query'
      required: false
      type: 'string'

    responses:
        200: 
            description: 'entries purged'
    """

    queue = dqueue.core.Queue()
    n = queue.purge()

    return jsonify(
            nentries=n
        )

@app.errorhandler(Exception)
def handle(error):
    logger.error("error: %s", repr(error))

    traceback.print_exc()


print("app added rules", id(app))
