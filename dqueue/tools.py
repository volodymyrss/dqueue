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


logger=logging.getLogger(__name__)

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

    return {k:v for k,v in bystate.items()}

def list_tasks(include_task_data=True):
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
        print("full entry:", entry)
        if entry['entry'] in decoded_entries:
            entry_data=decoded_entries[entry['entry']]
        else:
            try:
                entry_data=yaml.load(io.StringIO(entry['entry']), Loader=yaml.Loader)
                entry_data['submission_info']['callback_parameters']={}
                for callback in entry_data['submission_info'].get('callbacks', []):
                    if callback is not None:
                        entry_data['submission_info']['callback_parameters'].update(urllib.parse.parse_qs(callback.split("?",1)[1]))
                    else:
                        entry_data['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))
            except Exception as e:
                traceback.print_exc()
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
        return 

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
            formatted_exception=formatted_exception
        )
    except:
        r = entry['entry']

    db.close()
    return r

def purge():
    nentries=core.TaskEntry.delete().execute()
    return make_response("deleted %i"%nentries)

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

    return nentries


