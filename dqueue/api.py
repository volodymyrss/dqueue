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
    id = fields.Str()

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

class TaskView(SwaggerView):
    definitions = {
            'Task': Task, 
            'Status': Status, 
            "parameters_get" : [
                    {
                        'name': 'id',
                        'in': 'path',
                    }
                ], 
            "parameters_post" : [
                    {
                        'name': Task,
                        'in': 'query',
                    }
                ], 
            }

    def get(self, state="all"):
        """
        get task description
        ---
        parameters:
            schema:
              $ref: '#/definitions/parameters_get'
        responses:
          200:
            schema:
              $ref: '#/definitions/Task'
        """
        return jsonify(
                task_info()
            )

    def post(self, state="all"):
        """
        get task description
        ---
        parameters:
            schema:
              $ref: '#/definitions/parameters_post'
        responses:
          200:
            schema:
              $ref: '#/definitions/Status'
        """
        return jsonify(
                {'status': 'success'}
            )


app.add_url_rule(
         '/api/v1.0/tasks',
          view_func=TaskListView.as_view('api_tasks'),
          methods=['GET']
)

app.add_url_rule(
         '/api/v1.0/task',
          view_func=TaskView.as_view('api_task'),
          methods=['GET', 'POST']
)

print("app added rules", id(app))
