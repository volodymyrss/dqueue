import yaml
import datetime
import os
import time
import socket
from hashlib import sha224
from collections import OrderedDict, defaultdict
import glob
import logging
from io import StringIO
import re
import click
import urllib.parse as urlparse# type: ignore

from .core import Queue, Empty, Task

from bravado.client import SwaggerClient



class QueueProxy(Queue):
    master = None
    queue = None
    token = ""

    def __repr__(self):
        return f"[ {self.__class__.__name__}: {self.master}@{self.queue} ]"

    def __init__(self, queue_uri="http://localhost:5000@default"):
        super().__init__()

        r = re.search("(https?://.*?)@(.*)", queue_uri)
        if not r:
            raise Exception("uri does not match queue")

        self.master = r.groups()[0]
        self.queue = r.groups()[1]

    def list_queues(self, pattern):
        print(self.client.queues.list().response().result)
        return [QueueProxy(self.master+"@"+q) for q in self.client.queues.list().response().result]

    @property
    def client(self):
        if getattr(self, '_client', None) is None:
            self._client = SwaggerClient.from_url(self.master+"/apispec_1.json", config={'use_models': False})
        return self._client

    def find_task_instances(self,task,klist=None):
        raise NotImplementedError
    
    
    def select_task_entry(self,key):
        raise NotImplementedError
    
    def log_task(self,message,task=None,state=None):
        raise NotImplementedError


    def insert_task_entry(self,task,state):
        self.log_task("task created",task,state)

        log("to insert_task_entry: ", dict(
             queue=self.queue,
             key=task.key,
             state=state,
             worker_id=self.worker_id,
             entry=task.serialize(),
             created=datetime.datetime.now(),
             modified=datetime.datetime.now(),
        ))

        try:
            TaskEntry.insert(
                             queue=self.queue,
                             key=task.key,
                             state=state,
                             worker_id=self.worker_id,
                             entry=task.serialize(),
                             created=datetime.datetime.now(),
                             modified=datetime.datetime.now(),
                            ).execute()
        except (pymysql.err.IntegrityError, peewee.IntegrityError) as e:
            log("task already inserted, reasserting the queue to",self.queue)

            # deadlock
            TaskEntry.update(
                                queue=self.queue,
                            ).where(
                                TaskEntry.key == task.key,
                            ).execute()

    def put(self,task_data,submission_data=None, depends_on=None):
        print(dir(self.client.worker))

        return self.client.worker.questionTask(
                    worker_id=self.worker_id,
                    task_data=task_data,
                    token=self.token,
                    queue=self.queue,
                ).response().result


    def get(self):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        print(dir(self.client.worker))

        r = self.client.worker.getOffer(worker_id=self.worker_id, queue=self.queue, token=self.token).response()

        if r.result is None:
            raise Empty()

        self.current_task = Task.from_entry(r.result)

        return self.current_task


    def task_done(self):
        self.logger.info("task done, closing:",self.current_task.key,self.current_task)
        self.logger.info("task done, stored key:",self.current_task_stored_key)

    def clear_task_history(self):
        print('this is very descructive')
        TaskHistory.delete().execute()

    def task_failed(self,update=lambda x:None):
        update(self.current_task)

        task= self.current_task

        self.log_task("task failed",self.current_task,"failed")
        
        history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==task.key).order_by(TaskHistory.id.desc()).execute()]
        n_failed = len([he for he in history if he['state'] == "failed"])

        self.log_task("task failed %i times already"%n_failed,task,"failed")
        if n_failed < n_failed_retries:
            next_state = "waiting"
            self.log_task("task failure forgiven, to waiting",task,"waiting")
            time.sleep(5+2**int(n_failed/2))
        else:
            next_state = "failed"
            self.log_task("task failure permanent",task,"waiting")

        r=TaskEntry.update({
                    TaskEntry.state:next_state,
                    TaskEntry.entry:self.current_task.serialize(),
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute()

        self.current_task_status = next_state
        self.current_task = None

    def move_task(self,fromk,tok,task):
        r=TaskEntry.update({
                        TaskEntry.state:tok,
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .where(TaskEntry.state==fromk,TaskEntry.key==task.key).execute()

    def wipe(self,wipe_from=["waiting"]):
        for fromk in wipe_from:
            for key in self.list(fromk):
                log("removing",fromk + "/" + key)
                TaskEntry.delete().where(TaskEntry.key==key).execute()
        
    def purge(self):
        nentries = self.client.tasks.purge().response().result
        self.logger.info("deleted %s", nentries)


    def list(self, **kwargs):
        print(dir(self.client.tasks))
        l = [task for task in self.client.tasks.listTasks().response().result['tasks']]
        self.logger.info(f"found tasks: {len(l)}")
        return l

    @property
    def info(self):
        r={}
        tasks = self.list()
        for kind in "waiting","running","done","failed","locked":
            r[kind]=[t for t in tasks if t['state'] == kind]
        return r

    def show(self):
        r=""
        return r

    def resubmit(self, scope, selector):
        return self.client.tasks.resubmit(scope=scope, selector=selector)

    def watch(self,delay=1):
        while True:
            log(self.info())
            time.sleep(delay)

