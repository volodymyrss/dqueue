import yaml
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

import dqueue.core as core
from dqueue.core import model_to_dict

import peewee # type: ignore

from flask import Flask
from flask import render_template,make_response,request,jsonify
from flasgger import Swagger, SwaggerView, Schema, fields # type: ignore

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

    entries=[entry for entry in core.TaskEntry.select().where(core.TaskEntry.modified >= date_N_days_ago).order_by(core.TaskEntry.modified.desc()).execute(database=None)]

    bystate = defaultdict(int)
    #bystate = defaultdict(list)

    for entry in entries:
        print("found state", entry.state)
        bystate[entry.state] += 1
        #bystate[entry.state].append(entry)

    db.close()

    return {k:v for k,v in bystate.items()}



def decode_entry_data(entry):
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
        print("raw entry (undecodable)", entry['entry'])
        entry_data={'task_data':
                        {'object_identity':
                            {'factory_name':'??'}},
                    'submission_info':
                        {'callback_parameters':
                            {'job_id':['??'],
                             'session_id':['??']}}
                    }

    return entry_data


def list_tasks(include_task_data=True):
    try:
        db.connect()
    except peewee.OperationalError as e:
        pass

    output_json = request.args.get('json', False)

    pick_state = request.args.get('state', 'any')

    json_filter = request.args.get('json_filter')

    decode = bool(request.args.get('raw'))

    logger.info("searching for entries")
    date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=float(request.args.get('since',1)))

    if pick_state != "any":
        if json_filter:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.state == pick_state) & (core.TaskEntry.modified >= date_N_days_ago) & (core.TaskEntry.entry.contains(json_filter))).order_by(core.TaskEntry.modified.desc()).execute(database=None)]
        else:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.state == pick_state) & (core.TaskEntry.modified >= date_N_days_ago)).order_by(core.TaskEntry.modified.desc()).execute(database=None)]
    else:
        if json_filter:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where((core.TaskEntry.modified >= date_N_days_ago) & (core.TaskEntry.entry.contains(json_filter))).order_by(core.TaskEntry.modified.desc()).execute(database=None)]
        else:
            entries=[model_to_dict(entry) for entry in core.TaskEntry.select().where(core.TaskEntry.modified >= date_N_days_ago).order_by(core.TaskEntry.modified.desc()).execute(database=None)]

    logger.info("found entries %d",len(entries))

    for entry in entries:
        logger.info("decoding string of size %s",len(entry['entry']))
        logger.info("full entry keys: %s", entry.keys())
        if entry['entry'] not in decoded_entries:
            decoded_entries[entry['entry']] = decode_entry_data(entry)

        entry['entry'] = decoded_entries[entry['entry']]

    db.close()


    return entries

def task_info(key):
    entry=[model_to_dict(entry) for entry in core.TaskEntry.select().where(core.TaskEntry.key==key).execute(database=None)]
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
        
        history=[model_to_dict(en) for en in EventLog.select().where(EventLog.task_key==key).order_by(EventLog.id.desc()).execute(database=None)]
        #history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==key).order_by(TaskHistory.timestamp.desc()).execute(database=None)

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
    nentries=core.TaskEntry.delete().execute(database=None)
    return make_response("deleted %i"%nentries)

def resubmit(scope, selector):
    if scope=="state":
        if selector=="all":
            nentries=core.TaskEntry.update({
                            core.TaskEntry.state:"waiting",
                            core.TaskEntry.modified:datetime.datetime.now(),
                        })\
                        .execute(database=None)
        else:
            nentries=core.TaskEntry.update({
                            core.TaskEntry.state:"waiting",
                            core.TaskEntry.modified:datetime.datetime.now(),
                        })\
                        .where(core.TaskEntry.state==selector)\
                        .execute(database=None)
    elif scope=="task":
        nentries=core.TaskEntry.update({
                        core.TaskEntry.state:"waiting",
                        core.TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .where(core.TaskEntry.key==selector)\
                    .execute(database=None)

    return nentries


