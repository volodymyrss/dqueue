import traceback
import datetime
import os
import time
import socket
from hashlib import sha224
import glob
import logging
import io
import urllib.parse

import peewee # type: ignore

#from flask_httpauth import HTTPTokenAuth


from flask import Flask
from flask import render_template,make_response,request,jsonify

import dqueue.core as core
import dqueue.tools as tools
from dqueue.core import model_to_dict

import dqueue.api
import dqueue.database

logger=logging.getLogger(__name__)

app = Flask(__name__)

auth = None

#auth = HTTPTokenAuth(scheme='Bearer')

#@auth.verify_token
#def verify_token(token):
#    pass # jwt here
#    if token in tokens:
#        return tokens[token]


print("created app", id(app))

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

@app.before_request
def before_request():
    try:
        dqueue.database.db.connect()
        logger.debug("connecting to db before request %s", dqueue.database.db)
    except Exception as e:
        logger.error("db access error: %s", e)


@app.after_request
def after_request(response):
    dqueue.database.db.close()
    logger.debug("disconnecting from the db after request %s", dqueue.database.db)
    return response


@app.errorhandler(peewee.OperationalError)
def handle_dberror(e):
    logger.error("db access error: %s", e)
    logger.error(traceback.format_exc())
    return "server DB error! please contact me (you know how)!", 500

@app.route('/stats')
def stats():
    return render_template('task_stats.html', bystate=dqueue.tools.stats())


@app.route('/purge')
def purge():
    nentries=core.TaskEntry.delete().execute(database=None)
    return make_response("deleted %i"%nentries)

@app.route('/resubmit/<string:scope>/<string:selector>')
def resubmit(scope, selector):
    nentries = tools.resubmit(scope, selector)

    return make_response("resubmitted %i"%nentries)


@app.route('/task/info/<string:key>')
def task_info(key=None):
    r = render_template('task_info.html', **tools.task_info(key))
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

