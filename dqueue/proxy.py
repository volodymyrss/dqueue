import datetime
import os
import time
import json
import socket
from hashlib import sha224
from collections import defaultdict
import glob
import logging
from io import StringIO
import re
import click
from urllib.parse import urlparse# type: ignore

from dqueue.core import Queue, Empty, Task, CurrentTaskUnfinished
import dqueue.core as core
from dqueue import dqtyping
from typing import Union
from dqueue import tools

from retrying import retry # type: ignore

from bravado.client import SwaggerClient, RequestsClient

from dqueue.client import APIClient
from dqueue.data import DataFacts

class QueueProxy(DataFacts, Queue):
    
    def version(self):
        return self.client.hub.version().response().result

    def list_queues(self, pattern):
        print(self.client.queues.list().response().result)
        return [ QueueProxy(self.leader+"@"+q) for q in self.client.queues.list().response().result ]

    def find_task_instances(self,task,klist=None):
        raise NotImplementedError
    
    
    def select_task_entry(self,key):
        raise NotImplementedError
    
    def task_info(self, key):
        return self.client.task.task_info(task_key=key).response().result

    def task_by_key(self, key: str, decode: bool=False) -> dqtyping.TaskDict:
        r = self.client.task.task_info(task_key=key).response().result

        if decode:
            r['task_dict'] = tools.decode_entry_data(r)

        return r

    def clear_event_log(self, 
                        only_older_than_days: Union[float,None]=None, 
                        only_kind: Union[str,None]=None,
                        leave_last: Union[int,None]=None, 
                        ):
        return self.client.log.clear(only_older_than_days=only_older_than_days,
                                     only_kind=only_kind,
                                     leave_last=leave_last,
                                     ).response().result
    
    def view_log(self, task_key=None, since=0):
        if task_key is None:
            task_key = ""

        return self.client.log.view(task_key=task_key,
                                         since=since,
                                         ).response().result
    
    def log_queue(self, message, spent_s=0):
        self.logger.info("log queue %s", message)

        return self.client.worker.logQueue(message=message,
                                   spent_s=spent_s,
                                   worker_id=self.worker_id,
                                   ).response().result
    
    def log_task(self, message, task=None, state="unset", task_key=None):
        self.logger.info("log_task %s", message)

        if task_key is None:
            if task is None:
                task = self.current_task

            if task is None:
                task_key = "None"
            else:
                task_key = task.key

        def _log_task():
            return self.client.worker.logTask(message=message, 
                               task_key=task_key, 
                               state=state, 
                               queue=self.queue, 
                               worker_id=self.worker_id,
                               ).response().result

        def retry_on_exception(exception):
            self.logger.error("%s: error in client log_task: %s; trying to send %s %s %s %s %s",
                        self, 
                        exception,
                        message,
                        task_key,
                        state,
                        self.queue,
                        self.worker_id,
                    )
            return True

        return retry(
                    wait_exponential_multiplier=1000, 
                    wait_exponential_max=300000,
                    retry_on_exception=retry_on_exception,
                )(_log_task)()

    def insert_task_entry(self,task,state):
        raise NotImplementedError

    def put(self,task_data, submission_data=None, depends_on=None):
        return self.client.worker.questionTask(
                    worker_id=self.worker_id,
                    task_payload=dict(
                        task_data=task_data,
                        submission_data=submission_data,
                    ),
                    queue=self.queue,
                ).response().result


    def get(self, update_expected_in_s=-1):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        r = self.client.worker.getOffer(worker_id=self.worker_id, queue=self.queue, update_expected_in_s= update_expected_in_s).response()

        if r.result is None:
            raise Empty()

        self.current_task = Task.from_task_dict(r.result)
        self.current_task_stored_key = self.current_task.key

        return self.current_task


    def task_done(self):
        self.logger.info("task done, closing: %s : %s", self.current_task.key, self.current_task)
        self.logger.info("task done, stored key: %s", self.current_task_stored_key)
        self.logger.info("current task: %s", repr(self.current_task.as_dict)[:500]+" ...")

        r = self.client.worker.answer(worker_id=self.worker_id, 
                                      queue=self.queue, 
                                      task_dict=self.current_task.as_dict,
                                      ).response().result

        self.current_task = None

        return r

    def move_task(self, fromk, tok, task, update_entry=None, n_tries_left=0):
        if isinstance(task, str):
            task_key = task
        else:
            task_key = task.key

        if update_entry is None:
            update_entry = {}
        else:
            update_entry = json.loads(update_entry)

        self.logger.info("moving task %s from %s to %s", task_key, fromk, tok)
        self.logger.info("moving task uses update entry %s", repr(update_entry)[:300])

        r = self.client.tasks.move_task(
                                      #worker_id=self.worker_id, 
                                      #queue=self.queue, 
                                      task_key=task_key,
                                      fromk=fromk,
                                      tok=tok,
                                      update_entry=update_entry, # todo
                                      ).response().result

        self.current_task = None

        return r

    def clear_task_history(self):
        raise NotImplementedError

    def task_failed(self,update=lambda x:None):
        r = self.client.worker.failed(worker_id=self.worker_id, 
                                       queue=self.queue, 
                                       task_dict=self.current_task.as_dict,
                                      ).response().result
        self.current_task = None


    def wipe(self,wipe_from=["waiting"]):
        #for fromk in wipe_from:
        for key in self.list_tasks():
            self.logger.info("removing %s", key)
            core.TaskEntry.delete().where(core.TaskEntry.key==key).execute(database=None)
        
    def purge(self):
        nentries = self.client.tasks.purge().response().result
        self.logger.info("deleted %s", nentries)


    def list_tasks(self, state="any"):
        self.logger.warning("\033[31mexpensive list_tasks operation!\033[0m")
        l = [task for task in self.client.tasks.listTasks(state=state).response().result['tasks']]
        self.logger.info(f"found tasks: {len(l)}")
        return l

    @property
    def info(self):
        r={}
        tasks = self.list_tasks()
        for kind in "waiting","running","done","failed","locked":
            r[kind]=[t for t in tasks if t['state'] == kind]
        return r
    
    @property
    def summary(self):
        return self.client.tasks.summary(queue=self.queue).response().result['tasks']

    def show(self):
        r=""
        return r

    def resubmit(self, scope, selector):
        return self.client.tasks.resubmit(scope=scope, selector=selector).response().result

    def try_all_locked(self):
        return self.client.tasks.try_all_locked(worker_id=self.worker_id, queue=self.queue).response().result

    def forgive_task_failures(self):
        return self.client.tasks.forgive_failures(worker_id=self.worker_id, queue=self.queue).response().result
    
    def expire_tasks(self):
        return self.client.tasks.expire().response().result

    def callback(self, url, params):
        """
        """
        return self.client.worker.callback(
                    worker_id = self.worker_id,
                    payload=dict(
                            url = url,
                            params = params,
                        )
                ).response().result


