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
import json

import dqueue.core as core
from dqueue.database import model_to_dict, TaskEntry, EventLog
from dqueue.entry import decode_entry_data

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



def list_tasks(include_task_data=True, decode=True, state="any", json_filter=None):
    try:
        db.connect()
    except peewee.OperationalError as e:
        pass

    logger.info("searching for entries")
    date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=float(request.args.get('since',1)))

    c = core.TaskEntry.modified >= date_N_days_ago

    if state != "any":
        c &= core.TaskEntry.state == state

    if json_filter:
        c &= core.TaskEntry.task_dict_string.contains(json_filter)

    entries = [ model_to_dict(entry) for entry in core.TaskEntry.\
                                                  select().\
                                                  where(c).\
                                                  order_by(core.TaskEntry.modified.desc()).\
                                                  execute(database=None) ]

    logger.info("found entries %d",len(entries))


    if decode:
        t0=time.time()

        for entry in entries:
            if entry['key'] not in decoded_entries:
                logger.info("decoding string of size %s",len(entry['task_dict_string']))
                logger.info("full entry keys: %s", entry.keys())

                decoded_entries[entry['key']] = decode_entry_data(entry)

            entry['task_dict'] = decoded_entries[entry['key']]


        tspent = time.time()-t0

        logger.info("spent %f s decoding %d entries", tspent, len(entries))

    db.close()


    return entries

def task_info(key):
    raise NotImplementedError

    entry=[model_to_dict(entry) for entry in core.TaskEntry.select().where(core.TaskEntry.key==key).execute(database=None)]
    if len(entry)==0:
        return 

    entry=entry[0]

    print(("decoding",len(entry['entry'])))

    try:
        entry_data=json.loads(entry['entry'])
        entry['entry']=entry_data

        #from ansi2html import ansi2html# type: ignore

        if entry['entry']['execution_info'] is not None:
            entry['exception']=entry['entry']['execution_info']['exception']['exception_message']
        #    formatted_exception=ansi2html(entry['entry']['execution_info']['exception']['formatted_exception']).split("\n")
            formatted_exception=entry['exception']
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


