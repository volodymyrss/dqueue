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

from .core import Queue, Empty, Task, CurrentTaskUnfinished, TaskEntry

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
            self._client = SwaggerClient.from_url(self.master.strip("/")+"/apispec_1.json", config={'use_models': False})
        return self._client

    def find_task_instances(self,task,klist=None):
        raise NotImplementedError
    
    
    def select_task_entry(self,key):
        raise NotImplementedError
    
    def log_task(self,message,task=None,state=None):
        if task is None:
            task = self.current_task

        task_key = task.key

        self.client.worker.tasklog.logTask(message, task_key=task_key, state=state)

    def insert_task_entry(self,task,state):
        raise NotImplementedError

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
        self.current_task_stored_key = self.current_task.key

        return self.current_task


    def task_done(self):
        self.logger.info("task done, closing: %s : %s", self.current_task.key, self.current_task)
        self.logger.info("task done, stored key: %s", self.current_task_stored_key)
        self.logger.info("current task: %s", self.current_task.as_dict)

        r = self.client.worker.answer(worker_id=self.worker_id, 
                                      queue=self.queue, 
                                      token=self.token,
                                      task_dict=self.current_task.as_dict,
                                      ).response().result

        self.current_task = None

        return r

    def clear_task_history(self):
        raise NotImplementedError

    def task_failed(self,update=lambda x:None):
        raise NotImplementedError

    def move_task(self,fromk,tok,task):
        r=TaskEntry.update({
                        TaskEntry.state:tok,
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .where(TaskEntry.state==fromk, TaskEntry.key==task.key).execute()

    def wipe(self,wipe_from=["waiting"]):
        #for fromk in wipe_from:
        for key in self.list():
            self.logger.info("removing %s", key)
            TaskEntry.delete().where(TaskEntry.key==key).execute()
        
    def purge(self):
        nentries = self.client.tasks.purge().response().result
        self.logger.info("deleted %s", nentries)


    def list(self):
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

