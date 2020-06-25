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

import dqueue.core as core
from dqueue.core import model_to_dict

import peewee

from flask import Flask
from flask import render_template,make_response,request,jsonify
from flasgger import Swagger, SwaggerView, Schema, fields

decoded_entries={} # type: ignore

db = core.db


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
                tasks=list_tasks()
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

app = Flask(__name__)
swagger = Swagger(app)

app.add_url_rule(
         '/api/v1.0/tasks',
          view_func=TaskListView.as_view('tasks'),
          methods=['GET']
)

app.add_url_rule(
         '/api/v1.0/task',
          view_func=TaskView.as_view('task'),
          methods=['GET', 'POST']
)


logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler=logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
handler.setFormatter(formatter)

class ReverseProxied(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_FORWARDED_PREFIX', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)

app.wsgi_app = ReverseProxied(app.wsgi_app)# type: ignore

@app.errorhandler(peewee.OperationalError)
def handle_dberror(e):
    logger.error("db access error: %s", e)
    logger.error(traceback.format_exc())
    return "server DB error! please contact me (you know how)!", 500

@app.route('/stats')
def stats():
    try:
        db.connect()
    except peewee.OperationalError as e:
        pass

    decode = bool(request.args.get('raw'))

    print("searching for entries")
    date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=float(request.args.get('since',1)))

    entries=[entry for entry in core.TaskEntry.select().where(core.TaskEntry.modified >= date_N_days_ago).order_by(core.TaskEntry.modified.desc()).execute()]

    bystate = defaultdict(int)
    #bystate = defaultdict(list)

    for entry in entries:
        print("found state", entry.state)
        bystate[entry.state] += 1
        #bystate[entry.state].append(entry)

    db.close()

    if request.args.get('json') is not None:
        return jsonify({k:v for k,v in bystate.items()})
    else:
        return render_template('task_stats.html', bystate=bystate)
    #return jsonify({k:len(v) for k,v in bystate.items()})

def list_tasks():
    try:
        db.connect()
    except peewee.OperationalError as e:
        pass

    output_json = request.args.get('json', False)

    pick_state = request.args.get('state', 'any')

    json_filter = request.args.get('json_filter')

    decode = bool(request.args.get('raw'))

    print("searching for entries")
    date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=float(request.args.get('since',1)))

    if pick_state != "any":
        if json_filter:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.state == pick_state) & (core.TaskEntry.modified >= date_N_days_ago) & (core.TaskEntry.entry.contains(json_filter))).order_by(core.TaskEntry.modified.desc()).execute()]
        else:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.state == pick_state) & (core.TaskEntry.modified >= date_N_days_ago)).order_by(core.TaskEntry.modified.desc()).execute()]
    else:
        if json_filter:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.modified >= date_N_days_ago) & (core.TaskEntry.entry.contains(json_filter))).order_by(core.TaskEntry.modified.desc()).execute()]
        else:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where(core.TaskEntry.modified >= date_N_days_ago).order_by(core.TaskEntry.modified.desc()).execute()]


    print(("found entries",len(entries)))
    for entry in entries:
        print(("decoding",len(entry['entry'])))
        if entry['entry'] in decoded_entries:
            entry_data=decoded_entries[entry['entry']]
        else:
            try:
                entry_data=yaml.load(io.StringIO(entry['entry']))
                entry_data['submission_info']['callback_parameters']={}
                for callback in entry_data['submission_info']['callbacks']:
                    if callback is not None:
                        entry_data['submission_info']['callback_parameters'].update(urllib.parse.parse_qs(callback.split("?",1)[1]))
                    else:
                        entry_data['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))
            except Exception as e:
                print("problem decoding", repr(e))
                entry_data={'task_data':
                                {'object_identity':
                                    {'factory_name':'??'}},
                            'submission_info':
                                {'callback_parameters':
                                    {'job_id':['??'],
                                     'session_id':['??']}}
                            }


            decoded_entries[entry['entry']]=entry_data
        entry['entry']=entry_data

    db.close()


    return entries

def task_info(key):
    entry=[model_to_dict(entry) for entry in core.TaskEntry.select().where(core.TaskEntry.key==key).execute()]
    if len(entry)==0:
        return make_response("no such entry found")

    entry=entry[0]

    print(("decoding",len(entry['entry'])))

    try:
        entry_data=yaml.load(io.StringIO(entry['entry']))
        entry['entry']=entry_data

        from ansi2html import ansi2html# type: ignore

        if entry['entry']['execution_info'] is not None:
            entry['exception']=entry['entry']['execution_info']['exception']['exception_message']
            formatted_exception=ansi2html(entry['entry']['execution_info']['exception']['formatted_exception']).split("\n")
        else:
            entry['exception']="no exception"
            formatted_exception=["no exception"]
        
        history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==key).order_by(TaskHistory.id.desc()).execute()]
        #history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==key).order_by(TaskHistory.timestamp.desc()).execute()]

        r = dict(
            entry=entry,
            history=history,
            formatted_exception=formatted_exception)
        )
    except:
        r = entry['entry']

    db.close()
    return r

@app.route('/purge')
def purge():
    nentries=core.TaskEntry.delete().execute()
    return make_response("deleted %i"%nentries)

@app.route('/resubmit/<string:scope>/<string:selector>')
def resubmit(scope, selector):
    if scope=="state":
        if selector=="all":
            nentries=core.TaskEntry.update({
                            core.TaskEntry.state:"waiting",
                            core.TaskEntry.modified:datetime.datetime.now(),
                        })\
                        .execute()
        else:
            nentries=core.TaskEntry.update({
                            core.TaskEntry.state:"waiting",
                            core.TaskEntry.modified:datetime.datetime.now(),
                        })\
                        .where(core.TaskEntry.state==selector)\
                        .execute()
    elif scope=="task":
        nentries=core.TaskEntry.update({
                        core.TaskEntry.state:"waiting",
                        core.TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .where(core.TaskEntry.key==selector)\
                    .execute()

    return make_response("resubmitted %i"%nentries)


@app.route('/task/info/<string:key>')
def view_task_info(key):
    r = render_template('task_info.html', 
            entry=entry,
            history=history,
            formatted_exception=formatted_exception)
    return r


@app.route('/healthcheck')
@app.route('/')
def healthcheck():
    return jsonify(
                dict(
                        status="OK",
                        version="undefined",
                    )
            )

def listen():
    app.run(port=8000,debug=True,threaded=True)

if __name__ == "__main__":
    listen()

