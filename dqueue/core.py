import datetime
import os
import json
import time
import socket
from hashlib import sha224
from collections import OrderedDict, defaultdict
import glob
import logging
import io
import re
import click
import urllib.parse


from bravado.client import SwaggerClient

try:
    import io
except:
    from io import StringIO

try:
    import urlparse # type: ignore
except ImportError:
    import urllib.parse as urlparse# type: ignore

from typing import NewType, Dict, Union, List

import dqueue.typing as types
from dqueue.entry import decode_entry_data

import pymysql
import peewee # type: ignore
from playhouse.db_url import connect # type: ignore
from playhouse.shortcuts import model_to_dict, dict_to_model # type: ignore

sleep_multiplier = 1
n_failed_retries = int(os.environ.get('DQUEUE_FAILED_N_RETRY','20'))


def get_logger(name):
    level = getattr(logging, os.environ.get('DQUEUE_LOG_LEVEL', 'INFO'))

    logging.basicConfig(level=level)

    logger=logging.getLogger(name)
    logger.setLevel(level)

    handler=logging.StreamHandler()
    logger.addHandler(handler)

    formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
    handler.setFormatter(formatter)

    return logger

logger = get_logger(__name__)

def log(*args,**kwargs):
    severity=kwargs.get('severity','warning').upper()
    logger.log(getattr(logging,severity)," ".join([repr(arg) for arg in list(args)+list(kwargs.items())]))


class Empty(Exception):
    pass

class CurrentTaskUnfinished(Exception):
    pass

class TaskStolen(Exception):
    pass


def connect_db():
    return connect(os.environ.get("DQUEUE_DATABASE_URL","mysql+pool://root@localhost/dqueue?max_connections=42&stale_timeout=8001.2"))

try:
    db=connect_db()
    logger.info(f"successfully connected to db: {db}")
except Exception as e:
    logger.warning("unable to connect to DB: %s", repr(e))

    

class TaskEntry(peewee.Model):
    database = None

    queue = peewee.CharField(default="default")

    key = peewee.CharField(primary_key=True)
    state = peewee.CharField()
    worker_id = peewee.CharField()

    task_dict_string = peewee.TextField()

    created = peewee.DateTimeField()
    modified = peewee.DateTimeField()

    class Meta:
        database = db


class EventLog(peewee.Model):
    queue = peewee.CharField(default="default")

    task_key = peewee.CharField(default="unset")
    state = peewee.CharField(default="unset")

    worker_id = peewee.CharField()

    timestamp = peewee.DateTimeField()
    message = peewee.CharField()
    
    spent_s = peewee.FloatField(default=0)

    class Meta:
        database = db

try:
    db.create_tables([TaskEntry, EventLog])
    has_mysql = True
except peewee.OperationalError:
    has_mysql = False
except Exception:
    has_mysql = False

class Task:
    reference_task = False

    def __init__(self,task_data,execution_info=None, submission_data=None, depends_on=None):
        if 'task_key' in task_data:
            self._key = task_data['task_key']
            self.task_data = None
            self.reference_task = True
            logger.debug('creating referenced task %s from %s', self, task_data)
        else:
            self.task_data = task_data
            self.depends_on = depends_on

        self.submission_info=self.construct_submission_info()
        if submission_data is not None:
            self.submission_info.update(submission_data)

        self.execution_info=execution_info

    def construct_submission_info(self):
        return dict(
            time=time.time(),
            utc=time.strftime("%Y%m%d-%H%M%S"),
            hostname=socket.gethostname(),
            fqdn=socket.getfqdn(),
            pid=os.getpid(),
        )
    
    @property
    def as_dict(self):
        return dict(
                submission_info=self.submission_info,
                task_data=self.task_data,
                execution_info=self.execution_info,
                depends_on=self.depends_on,
            )

    @property
    def reference_dict(self):
        return dict(task_key=self.key)

    def serialize(self) -> str:
        return json.dumps(normalize_nested_dict(self.as_dict),
            sort_keys=True,
        )


    @classmethod
    def from_task_dict(cls, entry: Union[str, Dict]):
        if isinstance(entry, str):
            try:
                task_dict = json.loads(entry)
            except Exception as e:
                logger.error("problem decoding json from task entry: %s", e)
                for i, e_l in enumerate(entry.splitlines()):
                    print(f"problematic json: {i:5d}", e_l)
                open("/tmp/problematic_entry.json", "wt").write(entry)
                raise
        else:
            task_dict = entry

        if 'task_data' not in task_dict:
            logger.error("failed to build Task from entry %s", entry)
            logger.error("problematic task_dict %s", task_dict)
            logger.error("problematic task_dict keys %s", list(task_dict.keys()))
            raise RuntimeError(f"failed to build Task from entry {entry}")


        print(task_dict['task_data'].keys())

        self=cls(task_dict['task_data'])
        self.depends_on=task_dict.get('depends_on', [])
        self.submission_info=task_dict['submission_info']

        return self

        


    @property
    def key(self):
        return self.get_key(True)

    def get_key(self,key=True):
        if hasattr(self, '_key'):
            return self._key

        components = []

        task_data_string = json.dumps(order_nested_dict(self.task_data), sort_keys=True)

        logger.debug("task data: %s", self.task_data)
        logger.debug("task data string: %s", task_data_string)

        components.append(sha224(task_data_string.encode()).hexdigest()[:8])
        #log("encoding: "+repr(components))
        #log(task_data_string)
        #log("encoding: "+repr(components),severity="debug")
        #log(task_data_string,severity="debug")

        if not key:
            components.append("%.14lg"%self.submission_info['time'])
            components.append(self.submission_info['utc'])

            s = json.dumps(order_nested_dict(self.submission_info), sort_keys=True)
            components.append(sha224(s.encode()).hexdigest()[:8])

        key = "_".join(components)

        logger.warning("generating key %s", key)

        return key

    def __repr__(self):
        return "[{}: {}: {}]".format(self.__class__.__name__, self.key, self.task_data)

    def filename_instance(self):
        return "unset"

def makedir_if_neccessary(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != 17: raise

def list_queues(pattern=None):
    if pattern is None:
        return [ Queue(task_entry.queue) for task_entry in 
                TaskEntry.select(TaskEntry.queue).distinct() ]
    else:
        return [ Queue(task_entry.queue) for task_entry in 
                TaskEntry.select(TaskEntry.queue).where(TaskEntry.queue % pattern).distinct(TaskEntry.queue) ]

def normalize_nested_dict(d):
    if isinstance(d, dict) or isinstance(d, OrderedDict) or isinstance(d, defaultdict):
        return {
                k:normalize_nested_dict(v) for k, v in sorted(d.items())
            }

    if isinstance(d, tuple) or isinstance(d, list):
        return [ normalize_nested_dict(i) for i in d ]

    return d


def order_nested_dict(d):
    if isinstance(d, dict) or isinstance(d, OrderedDict) or isinstance(d, defaultdict):
        return OrderedDict({
                k:order_nested_dict(v) for k, v in sorted(d.items())
            })

    if isinstance(d, tuple) or isinstance(d, list):
        return [order_nested_dict(i) for i in d]

    return d


class Queue:
    def list_queues(self, pattern=None):
        return list_queues(pattern)

    def __init__(self,queue="default", worker_id=None):
        ""
        self.logger = logging.getLogger(repr(self))

        if worker_id is None:
            self.worker_id=self.get_worker_id()
        else:
            self.worker_id=worker_id

        self.queue=queue
        self.current_task=None
        self.current_task_status=None

    def find_task_instances(self, task: Task, klist: Union[list, None]=None) -> List[types.TaskEntry]:
        ""
        log("find_task_instances for",task.key,"in",self.queue)

        if klist is None:
            klist=["waiting", "running", "done", "failed", "locked"]

        instances_for_key=[
                model_to_dict(task_entry) for task_entry in TaskEntry.select().where(TaskEntry.state << klist, TaskEntry.key==task.key, TaskEntry.queue==self.queue)
            ]

        log("found task instances for",task.key,"N == ",len(instances_for_key))
        for i in instances_for_key:
            log(i['state'], i['task_dict_string'])

        return instances_for_key
    
    def try_to_unlock(self,task):
        ""
        dependency_states=self.find_dependecies_states(task)
        

        if all([d['state']=="done" for d in dependency_states]):
            self.log_task("task dependencies complete: unlocking",task,"locked")
            log("dependecies complete, will unlock", task)
            self.move_task("locked", "waiting", task)
            return dict(state="waiting", key=task.key)

        if any([d['state']=="failed" for d in dependency_states]):
            log("dependecies complete, will unlock", task)
            self.log_task("task dependencies failed: unlocking to fail",task,"failed")
            self.move_task("locked", "failed", task)
            return dict(state="failed", key=task.key)

        if not any([d['state'] in ["running","waiting","locked","incomplete"] for d in dependency_states]):
            log("dependecies incomplete, but nothing will come of this anymore, will unlock", task)
            #self.log_task("task dependencies INcomplete: unlocking",task,"locked")

            from collections import defaultdict
            dd=defaultdict(int)
            for d in dependency_states:
                dd[d['state']]+=1

            self.log_task("task dependencies INcomplete: "+repr(dict(dd)),task,"locked")
           # self.move_task("locked", "waiting", task)
          #  return dict(state="waiting", key=task.key)

        log("task still locked", task.key)
        return dict(state="locked",key=task.key)
    
    def task_by_key(self, key: str, decode: bool=False) -> Union[types.TaskDict, None]:
        ""

        r=TaskEntry.select().where(
                         TaskEntry.key==key,
                        ).execute(database=None)

        if len(r) > 1:
            raise RuntimeError(f"found multiple entries for key {key}: suspecting database inconsistency!")
        
        if len(r) == 0:
            return None

        r = model_to_dict(r[0])

        if decode:
            decode_entry_data(r)

        return r


    
    def put(self, task_data: types.TaskData, submission_data=None, depends_on=None) -> Union[types.TaskEntry, None]:
        logger.info("putting in queue task_data %s", task_data)

        assert depends_on is None or type(depends_on) in [list, tuple] # runtime typing!

        if depends_on is not None:
            depends_on = [ Task(dep).reference_dict for dep in depends_on ]

        task=Task(task_data, submission_data=submission_data, depends_on=depends_on)

        instances_for_key = None # type: Union[List[types.TaskEntry], None]

        ntry_race=10
        retry_sleep_race=2
        while ntry_race>0:
            instances_for_key = self.find_task_instances(task) 
            log("found instances for key:",instances_for_key)

            if instances_for_key is not None:
                if len(instances_for_key)<=1:
                    break

                logger.error("found unexpected number of instances for key: %d", len(instances_for_key))
            else:
                logger.error("instances_for_key is None!")

            log("sleeping for",retry_sleep_race,"attempt",ntry_race)
            time.sleep(retry_sleep_race)
            ntry_race-=1

        if instances_for_key is None or len(instances_for_key)>1:
            raise Exception("probably race condition, multiple task instances:",instances_for_key)

        instance_for_key = None # type: Union[types.TaskEntry, None]
        if len(instances_for_key) == 1:
            instance_for_key = instances_for_key[0]

        if instance_for_key is not None:
            log("found existing instance(s) for this key, no need to put:", instances_for_key)
            self.log_task("task already found", task,instance_for_key['state'])
            d = instance_for_key
            logger.debug("task entry: %s", d)
            return d

        if depends_on is None:
            self.insert_task_entry(task, "waiting")
            log("task inserted as waiting")
        else:
            self.insert_task_entry(task, "locked")
            log("task inserted as locked")

        instance_for_key = self.find_task_instances(task)[0]
        recovered_task = Task.from_task_dict(instance_for_key['task_dict_string']) # type: ignore

        if recovered_task.key != task.key:
            log("inconsitent storage:")
            log("stored:",task.filename_instance)
            log("recovered:", recovered_task.filename_instance)
    
            #nfn=self.queue_dir("conflict") + "/put_original_" + task.filename_instance
            #open(nfn, "w").write(task.serialize())
        
            #nfn=self.queue_dir("conflict") + "/put_recovered_" + recovered_task.filename_instance
            #open(nfn, "w").write(recovered_task.serialize())
            
            #nfn=self.queue_dir("conflict") + "/put_stored_" + os.path.basename(fn)
            #open(nfn, "w").write(open(fn).read())

            raise Exception("Inconsistent storage")

        logger.debug("successfully put in queue: %s",instance_for_key['task_dict_string'])

        
        instance_for_key['state'] = 'submitted'
        return instance_for_key

    def get(self):
        ""
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

    
        r=TaskEntry.update({
                        TaskEntry.state:"running",
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .order_by(TaskEntry.created)\
                    .where( (TaskEntry.state=="waiting") & (TaskEntry.queue==self.queue) ).limit(1).execute(database=None)

        if r==0:
            #self.try_all_locked()
            raise Empty()

        entries=TaskEntry.select().where(TaskEntry.worker_id==self.worker_id,TaskEntry.state=="running").order_by(TaskEntry.modified.desc()).limit(1).execute(database=None)
        if len(entries)>1:
            raise Exception(f"several tasks ({len(entries)}) are running for this worker: impossible!")

        entry=entries[0]
        self.current_task=Task.from_task_dict(entry.task_dict_string)
        self.current_task_stored_key=self.current_task.key

        if self.current_task.key != entry.key:
            logger.error("current task key computed now does not match that found in record")
            logger.error("current task key: %s task: %s", self.current_task.key, self.current_task)
            logger.error("fetched task key: %s entry: %s", entry.key, entry)

        log(self.current_task.key)
        

        if self.current_task.key != entry.key:
            log("inconsitent storage:")
            log(">>>> stored:", entry)
            log(">>>> recovered:", self.current_task)

            #fn=self.queue_dir("conflict") + "/get_stored_" + self.current_task.filename_instance
            #open(fn, "w").write(self.current_task.serialize())
        
            #fn=self.queue_dir("conflict") + "/get_recovered_" + task_name
            #open(fn, "w").write(open(self.queue_dir("waiting")+"/"+task_name).read())

            raise Exception("Inconsistent storage")


        log("task is running",self.current_task)
        self.current_task_status = "running"

        self.log_task("task started")

        log('task',self.current_task.submission_info)

        return self.current_task
    
    def clear_task_history(self):
        # compatibility
        return self.clear_event_log()

    def clear_event_log(self):
        ""
        print('this is very descructive')
        EventLog.delete().execute(database=None)

    def move_task(self, fromk, tok, task, update_entry=False):
        ""

        extra = {}
        if update_entry:
            extra = {TaskEntry.task_dict_string: self.current_task.serialize()}

        r=TaskEntry.update({
                        TaskEntry.state:tok,
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                        **extra
                    })\
                    .where(TaskEntry.state==fromk, TaskEntry.key==task.key).execute(database=None)

    def purge(self):
        ""
        nentries=TaskEntry.delete().execute(database=None)
        log("deleted %i"%nentries)

        return nentries

    
    def try_all_locked(self, unlock_max = 10):
        ""
        r=[]

        n_unlocked = 0

        locked_tasks = self.list_tasks(state="locked")

        logger.info("found %d locked tasks", len(locked_tasks))

        for task_key in locked_tasks:
            task_entry = self.task_by_key(task_key)
            logger.info("trying to unlock %s", task_entry['key'])

            r.append(self.try_to_unlock(Task.from_task_dict(task_entry['task_dict_string'])))

            if r[-1]['state'] != "locked":
                n_unlocked += 1

            if n_unlocked >= unlock_max:
                break

        return r
    
    def remember(self,task_data,submission_data=None):
        ""
        task=Task(task_data,submission_data=submission_data)
        #nfn=self.queue_dir("problem") + "/"+task.filename_instance
        #open(nfn, "w").write(task.serialize())
            

    def insert_task_entry(self,task,state):
        self.log_task("task created",task,state)

        log("to insert_task entry: ", dict(
             queue=self.queue,
             key=task.key,
             state=state,
             worker_id=self.worker_id,
             task_dict_string=task.serialize(),
             created=datetime.datetime.now(),
             modified=datetime.datetime.now(),
        ))

        try:
            TaskEntry.insert(
                             queue=self.queue,
                             key=task.key,
                             state=state,
                             worker_id=self.worker_id,
                             task_dict_string=task.serialize(),
                             created=datetime.datetime.now(),
                             modified=datetime.datetime.now(),
                            ).execute(database=None)
        except (pymysql.err.IntegrityError, peewee.IntegrityError) as e:
            log("task already inserted, reasserting the queue to",self.queue)

            # deadlock
            TaskEntry.update(
                                queue=self.queue,
                            ).where(
                                TaskEntry.key == task.key,
                            ).execute(database=None)


    def find_dependecies_states(self,task):
        if task.depends_on is None:
            raise Exception("can not inspect dependecies in an independent task!")

        log("find_dependecies_states for",task.key)

        dependencies=[]
        for i_dep, dependency in enumerate(task.depends_on):
            dependency_task=Task(dependency)

            print(("task",task.key,"depends on task", dependency_task.key, i_dep, "/", len(task.depends_on)))
            dependency_instances=self.find_task_instances(dependency_task)
            print(("task instances for",dependency_task.key,len(dependency_instances)))

            dependencies.append(dict(states=[]))

            for i_i,i in enumerate(dependency_instances):
                # if i['state']=="done"]) == 0:
                #log("dependency incomplete")
                dependencies[-1]['states'].append(i['state'])
                dependencies[-1]['task']=dependency_task
                print(("task instance for",dependency_task.key,"is",i['state'],"from",i_i,"/",len(dependency_instances)))

            if len(dependencies[-1]['states'])==0:
                print(("job dependencies do not exist, expecting %s"%dependency_task.key))
                #print(dependency_task.serialize())
                raise Exception("job dependencies do not exist, expecting %s"%dependency_task.key)

            if 'done' in dependencies[-1]['states']:
                dependencies[-1]['state']='done'
            elif 'failed' in dependencies[-1]['states']:
                dependencies[-1]['state']='failed'
            else:
                dependencies[-1]['state']='incomplete'
            
            try:
                log("dependency:",dependencies[-1]['state'],dependencies[-1]['states'], dependencies[-1]['task'].key, dependency_instances[0])
            except KeyError:
                log("problematic dependency:",dependencies[-1])
                raise Exception("problematic dependency:",dependencies[-1])
            #log("dependency:",dependencies[-1]['state'],dependencies[-1]['states'], dependency, dependency_instances)

        return dependencies




    def task_locked(self, depends_on: List[types.TaskDict]):
        ""
        if not isinstance(depends_on, list):
            raise Exception(f"depends_on has unknown type {depends_on.__class__}, expected list")

        log("locking task",self.current_task)
        self.log_task(f"task to lock by {len(depends_on)} dependencies",state="locked")
        if self.current_task is None:
            raise Exception("task must be available to lock")

        self.current_task.depends_on = [] 
        
        for dependency in depends_on:
            dependency_key = Task(dependency).key

            #dependency_task = self.task_by_key(dependency_key)

            dependency_reference = dict(
                        task_key=dependency_key,
                    )

            self.current_task.depends_on.append(dependency_reference)

        serialized=self.current_task.serialize()

        self.log_task("task to lock: serialized to %i"%len(serialized), state="locked")

        n_tries_left=10
        retry_delay=2
        while n_tries_left>0:
            try:

                self.move_task('running', 'locked', self.current_task, update_entry=True)

            except Exception as e:
                log('failed to lock:',repr(e))
                self.log_task("task to failed lock: %s; serialized to %i"%(repr(e),len(serialized)),state="failed_to_lock")
                time.sleep(retry_delay)
                if n_tries_left==1:
                    raise
                n_tries_left-=1
            else:
                break
        
        self.log_task("task locked from "+str(self.current_task_status),state="locked")

        self.current_task_status="locked"
        self.current_task=None


    def task_done(self):
        log("task done, closing:",self.current_task.key,self.current_task)
        log("task done, stored key:",self.current_task_stored_key)

        self.log_task("task to register done")

        r=TaskEntry.update({
                    TaskEntry.state:"done",
                    TaskEntry.task_dict_string:self.current_task.serialize(), # TODO this modifies serialization!
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute(database=None)

        if self.current_task_stored_key != self.current_task.key:
            r=TaskEntry.update({
                        TaskEntry.state:"done",
                        TaskEntry.task_dict_string:self.current_task.serialize(),
                        TaskEntry.modified:datetime.datetime.now(),
                    }).where(TaskEntry.key==self.current_task_stored_key).execute(database=None)


        self.current_task_status="done"

        self.log_task("task done")
        log('task registered done',self.current_task.key)

        self.current_task=None


    def task_failed(self,update=lambda x:None):
        update(self.current_task)

        task= self.current_task

        self.log_task("task failed",self.current_task,"failed")
        
        history=[model_to_dict(en) for en in EventLog.select().where(EventLog.task_key==task.key).order_by(EventLog.id.desc()).execute(database=None)]
        n_failed = len([he for he in history if he['state'] == "failed"])

        self.log_task("task failed %i times already"%n_failed,task,"failed")
        if n_failed < n_failed_retries:
            next_state = "waiting"
            self.log_task("task failure forgiven, to waiting",task,"waiting")
            time.sleep( (5+2**int(n_failed/2))*sleep_multiplier )
        else:
            next_state = "failed"
            self.log_task("task failure permanent",task,"waiting")

        r=TaskEntry.update({
                    TaskEntry.state:next_state,
                    TaskEntry.task_dict_string:self.current_task.serialize(),
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute(database=None)

        self.current_task_status = next_state
        self.current_task = None


    def wipe(self,wipe_from=["waiting"]):
        for fromk in wipe_from:
            for key in self.list_tasks(fromk):
                log("removing",fromk + "/" + key)
                TaskEntry.delete().where(TaskEntry.key==key).execute(database=None)
        
    @property
    def info(self):
        ""
        r={}
        for kind in "waiting","running","done","failed","locked":
            r[kind]=len(self.list_tasks(kind))
        return r

    def show(self):
        ""
        r=""
        for kind in "waiting","running","done","failed","locked":
            r+="\n= "+kind+"\n"
            for task_entry in TaskEntry.select().where(TaskEntry.state==kind, TaskEntry.queue==self.queue):
                r+=" - "+repr(model_to_dict(task_entry))+"\n"
        return r


    def watch(self,delay=1):
        """"""
        while True:
            log(self.info)
            time.sleep(delay)

    def get_worker_id(self):
        ""
        d=dict(
            time=time.time(),
            utc=time.strftime("%Y%m%d-%H%M%S"),
            hostname=socket.gethostname(),
            fqdn=socket.getfqdn(),
            pid=os.getpid(),
        )
        return "{fqdn}.{pid}".format(**d)
    

    def view_log(self, task_key=None, since=0):
        c = EventLog.id >= since

        if task_key is not None:
            c &= EventLog.task_key==task_key

        history=[ model_to_dict(en) for en in EventLog.select()\
                                                      .where(c)\
                                                      .order_by(EventLog.id.asc())\
                                                      .execute(database=None) ]

        return history
    
    def log_queue(self, message, spent_s, worker_id):
        ""
        return EventLog.insert(
                             queue=self.queue,
                             worker_id=self.worker_id,
                             timestamp=datetime.datetime.now(),
                             message=message,
                             spent_s=spent_s,
                        ).execute(database=None)


    def log_task(self, message, task=None, state=None, task_key=None):
        ""

        if task_key is not None:
            key=task_key
        else:
            if task is None:
                task=self.current_task

            task_key = task.key

        if state is None:
            state="undefined"

        return EventLog.insert(
                             queue=self.queue,
                             task_key=task_key,
                             state=state,
                             worker_id=self.worker_id,
                             timestamp=datetime.datetime.now(),
                             message=message,
                        ).execute(database=None)


    def list(self, *args, **kwargs): # compatibility
        logger.warning("please use list_tasks instead")
        return self.list_tasks(*args, **kwargs)

    def list_tasks(self, state=None, states=None, kind=None, kinds=None, decode=False):
        ""

        if kind is not None:
            logger.warning("please use 'state' instead of 'kind' in list_tasks")
            state = kind

        if state is not None:
            states=[state]

        if kinds is not None:
            logger.warning("please use 'states' instead of 'kinds' in list_tasks")
            states = kinds

        if states is None:
            states=["waiting"]


        jobs = []

        logger.info("requested to list_tasks; queue: %s states: %s", self.queue, states)

        for state in states:
            for task_entry in TaskEntry.select().where(TaskEntry.state==state, TaskEntry.queue==self.queue):
                jobs.append(task_entry.key)

        return jobs


