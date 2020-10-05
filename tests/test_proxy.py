import pytest
import json
from flask import url_for
import time

import logging
logging.basicConfig(level=logging.DEBUG)

import dqueue.core as core
from dqueue.core import Queue
from dqueue import from_uri

def test_direct(client):
    r = client.get("tasks").json

    print(r)

@pytest.mark.usefixtures('live_server')
class TestLiveServer:
    @property
    def local_queue(self):
        if not hasattr(self, '_local_queue'):
            u = url_for("healthcheck", _external=True)
            print("u:", u)
            self._local_queue = Queue("default")

        return self._local_queue

    @property
    def queue(self):
        if not hasattr(self, '_queue'):
            u = url_for("healthcheck", _external=True)
            print("u:", u)
            self._queue = from_uri(u+"@default")

        return self._queue

    def test_construct(self):
        tl = self.queue.list()
        print(tl)

        self.queue.purge()

        assert self.queue.version()['version'] == core.__version__
    
    
    def test_offer(self):
        self.queue.purge()

        len(self.queue.list()) == 0

        td = {'1':'2'}

        self.local_queue.put(td, {})
        r = self.queue.put(td, {})

        print("put returns", r)
        rd = json.loads(r['task_dict_string'])

        print("put returns", rd)

        assert rd['task_data'] == td
        
        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'waiting'

        to = self.queue.get()

        task = self.queue.current_task

        
        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'running'

        print(to)

        assert to.as_dict['task_data'] == td

        

        task_done_r = self.queue.task_done()

        assert task_done_r['task_key'] == task.key

        l = self.queue.list()
        len(l) == 1
            
        assert l[0]['state'] == 'done'

        self.queue.log_task("test log task", task_key="12345", state="none")

        lg = self.queue.view_log()
        print("unspecificed:", lg)
        assert len(lg['event_log'])>0
        

        lg = self.queue.view_log(task_key="12345")

        print(lg)
        assert len(lg['event_log'])>0
    
        print("task", l[0])
        key = l[0]['key']

        
        lg = self.queue.view_log()
        print(lg)

        n = len(lg['event_log'])

        self.queue.log_queue("test.nothing", 1.5)
        lg = self.queue.view_log(task_key=None)
        print("queue log", lg)
        assert len(lg['event_log'])>0
        assert len(lg['event_log']) == n + 1

        s = lg['event_log'][-1]['id']+1

        lg = self.queue.view_log(task_key=None, since=s)
        assert len(lg['event_log']) == 0
        
        self.queue.log_queue("test.nothing", 1.5)

        lg = self.queue.view_log(task_key=None, since=s)
        assert len(lg['event_log']) == 1


        # move
    
        l = self.queue.list()
        len(l) == 1
        k = l[-1]['key']
        
        l = self.queue.list()
        print("queue list before move", l)

        lg = self.queue.move_task(task=k, fromk="done", tok="waiting")

        l = self.queue.list()
        print("queue list after move", l)

        assert l[0]['key'] == k
        assert l[0]['state'] == "waiting"


        # fail
        l = self.queue.list()
        len(l) == 1
        k = l[-1]['key']

        to = self.queue.get()
        lg = self.queue.task_failed()

        l = self.queue.list()
        print("queue list after failed", l)

        assert l[0]['key'] == k
        assert l[0]['state'] == "failed"
        
        self.queue.forgive_task_failures()
        
        l = self.queue.list()
        print("queue list after failed", l)

        assert l[0]['key'] == k
        assert l[0]['state'] == "waiting"
        
        to = self.queue.get()
    
    def test_expire(self):
        self.queue.purge()

        len(self.queue.list()) == 0

        td = {'1':'2'}

        r = self.queue.put(td, {})

        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'waiting'

        to = self.queue.get(2.)

        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'running'

        time.sleep(1.5)
        self.queue.expire_tasks()
        
        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'running'
        
        time.sleep(1.5)
        self.queue.expire_tasks()
        
        l = self.queue.list()
        len(l) == 1
        assert l[0]['state'] == 'failed'

    def test_callback(self):
        r = self.queue.callback(
                    url=url_for("healthcheck", _external=True),
                    params={},
                )


