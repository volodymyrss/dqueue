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

from flask import Flask
from flask import render_template,make_response,request,jsonify
from flasgger import Swagger, SwaggerView, Schema, fields

decoded_entries={} # type: ignore

db = dqueue.core.db

app = dqueue.app.app
swagger = Swagger(app)

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

class Status(Schema):
    status = fields.Str()

## === views

class TaskListView(SwaggerView):
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
                tasks=tools.list_tasks()
            )

app.add_url_rule(
         '/tasks',
          view_func=TaskListView.as_view('api_tasks'),
          methods=['GET']
)

class WorkerOffer(SwaggerView):
    parameters = [
                {
                    'name': 'worker_id',
                    'in': 'query',
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

    def get(self):
        queue = dqueue.core.Queue()
        task = queue.get()
        logger.warning("got task: %s", task)
        return jsonify(
                task_data=task.task_data,
            )

app.add_url_rule(
         '/worker/offer',
          view_func=WorkerOffer.as_view('worker_offer_task'),
          methods=['GET']
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


@app.route("/tasks/purge")
def tasks_purge():
    """
    ---
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
