import datetime
import os
import time
import socket
from hashlib import sha224
from collections import defaultdict
import glob
import logging
from io import StringIO
import re
import click
import urllib.parse as urlparse# type: ignore

from dqueue.core import Queue, Empty, Task, CurrentTaskUnfinished
import dqueue.core as core
import dqueue.typing as types
from dqueue import tools

from bravado.client import SwaggerClient



class QueueProxy(Queue):
    leader = None
    queue = None
    token = ""

    def __repr__(self):
        return f"[ {self.__class__.__name__}: {self.leader}@{self.queue} ]"

    def __init__(self, queue_uri="http://localhost:5000@default"):
        super().__init__()

        r = re.search("(https?://.*?)@(.*)", queue_uri)
        if not r:
            raise Exception("uri does not match queue")

        self.leader = r.groups()[0]
        self.queue = r.groups()[1]

    def list_queues(self, pattern):
        print(self.client.queues.list().response().result)
        return [QueueProxy(self.leader+"@"+q) for q in self.client.queues.list().response().result]

    @property
    def client(self):
        if getattr(self, '_client', None) is None:
            self._client = SwaggerClient.from_url(self.leader.strip("/")+"/apispec_1.json", config={'use_models': False})
        return self._client

    def find_task_instances(self,task,klist=None):
        raise NotImplementedError
    
    
    def select_task_entry(self,key):
        raise NotImplementedError
    
    def task_info(self, key):
        return self.client.task.task_info(task_key=key).response().result

    def task_by_key(self, key: str, decode: bool=False) -> types.TaskDict:
        r = self.client.task.task_info(task_key=key).response().result

        if decode:
            r['task_dict'] = tools.decode_entry_data(r)

        return r
    
    def view_log(self, task_key=None, since=0):
        if task_key is None:
            task_key = ""

        return self.client.task.view_log(task_key=task_key,
                                         since=since,
                                         token=self.token).response().result
    
    def log_queue(self, message, spent_s=0):
        self.logger.info("log queue %s", message)

        return self.client.worker.logQueue(message=message,
                                   spent_s=spent_s,
                                   worker_id=self.worker_id,
                                   token=self.token).response().result
    
    def log_task(self, message, task=None, state=None, task_key=None):
        self.logger.info("log_task %s", message)

        if task_key is None:
            if task is None:
                task = self.current_task

            task_key = task.key

        return self.client.worker.logTask(message=message, 
                                   task_key=task_key, 
                                   state=state, 
                                   queue=self.queue, 
                                   worker_id=self.worker_id,
                                   token=self.token).response().result

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

        self.current_task = Task.from_task_dict(r.result)
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


    def wipe(self,wipe_from=["waiting"]):
        #for fromk in wipe_from:
        for key in self.list_tasks():
            self.logger.info("removing %s", key)
            core.TaskEntry.delete().where(core.TaskEntry.key==key).execute(database=None)
        
    def purge(self):
        nentries = self.client.tasks.purge().response().result
        self.logger.info("deleted %s", nentries)


    def list_tasks(self):
        l = [task for task in self.client.tasks.listTasks().response().result['tasks']]
        self.logger.info(f"found tasks: {len(l)}")
        return l

    @property
    def info(self):
        r={}
        tasks = self.list_tasks()
        for kind in "waiting","running","done","failed","locked":
            r[kind]=[t for t in tasks if t['state'] == kind]
        return r

    def show(self):
        r=""
        return r

    def resubmit(self, scope, selector):
        return self.client.tasks.resubmit(scope=scope, selector=selector)

    def try_all_locked(self):
        return self.client.tasks.try_all_locked(worker_id=self.worker_id, token=self.token, queue=self.queue).response().result

