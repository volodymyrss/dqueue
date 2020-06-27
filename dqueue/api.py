import yaml
import traceback
import datetime
import os
import time
import socket
from hashlib import sha224
from collections import OrderedDict, defaultdict
import glob
import logging
import io
import urllib.parse

import dqueue.core 
import dqueue.app
import dqueue.tools as tools

import peewee
import json

from flask import Flask
from flask import render_template,make_response,request,jsonify
from flasgger import Swagger, SwaggerView, Schema, fields

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

logger=logging.getLogger(__name__)


## === schemas

class Task(Schema):
    state = fields.Str()
    queue = fields.Str()
    task_id = fields.Str()
    task_data = fields.Dict()

class TaskList(Schema):
    tasks = fields.Nested(Task, many=True)

class QueueList(Schema):
    queues = fields.Nested(fields.Str(), many=True)

class Status(Schema):
    status = fields.Str()

## === views

class TaskListView(SwaggerView):
    operationId = "listTasks"
    parameters = [
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

        return jsonify(
                tasks=[e for e in tools.list_tasks(include_task_data=True)]
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

    def get(self):
        queue = dqueue.core.Queue(request.args.get('queue', 'default'))

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
         '/worker/offer',
          view_func=WorkerOffer.as_view('worker_offer_task'),
          methods=['GET']
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
                    'schema': Task, 
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
                    'description': 'task data',
                }
        }

    def post(self):
        queue = request.args.get('queue', 'default')
        worker_id = request.args.get('worker_id')
        task_data = request.json

        queue = dqueue.core.Queue(worker_id=worker_id, queue=queue)

        print("got:", worker_id, task_data)

        task = queue.put(task_data)

        logger.warning("questioned task: %s", task)
        return jsonify(
                {}
            )

app.add_url_rule(
     '/worker/question',
      view_func=WorkerQuestion.as_view('worker_question_task'),
      methods=['POST']
)

class TaskView(SwaggerView):
    parameters = [
                {
                    'name': 'task_id',
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

    def get(self, task_id):
        info = tools.task_info(task_id)
        logger.warning("requested task_id %s %s", task_id, info)
        return jsonify(
                task_id=task_id,
                task_info=info,
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


app.add_url_rule(
         '/task/view/<task_id>',
          view_func=TaskView.as_view('api_task'),
          methods=['GET']
)

print("app added rules", id(app))
