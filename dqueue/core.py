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

from functools import reduce

from bravado.client import SwaggerClient

__version__ = "0.1.26"

try:
    import io
except:
    from io import StringIO

try:
    import urlparse # type: ignore
except ImportError:
    import urllib.parse as urlparse# type: ignore

from typing import NewType, Dict, Union, List

import dqueue.dqtyping as dqtyping
from dqueue.entry import decode_entry_data

import pymysql
import peewee # type: ignore

from dqueue.database import EventLog, TaskEntry, db, model_to_dict

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


        short_name = "unknown"

        if 'object_identity' in self.task_data:
            short_name = self.task_data['object_identity']['full_name']

        logger.warning("generating key %s short name %s", key, short_name)

        return key

    def __repr__(self):
        return "[{}: {}: {}]".format(self.__class__.__name__, self.key, repr(self.task_data)[:300])

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
    current_task=None
    queue=None

    def __repr__(self):
        return f"[{self.__class__.__name__}: {self.queue} @ {self.current_task} ]"

    def list_queues(self, pattern=None):
        return list_queues(pattern)

    def __init__(self,queue="default", worker_id=None):
        ""

        if worker_id is None:
            self.worker_id=self.get_worker_id()
        else:
            self.worker_id=worker_id

        self.queue=queue
        self.current_task=None
        self.current_task_status=None
        self.logger = logging.getLogger(repr(self))

    def find_task_instances(self, task: Task, klist: Union[list, None]=None) -> List[dqtyping.TaskEntry]:
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
    
    def task_by_key(self, key: str, decode: bool=False) -> Union[dqtyping.TaskDict, None]:
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


    
    def put(self, task_data: dqtyping.TaskData, submission_data=None, depends_on=None) -> Union[dqtyping.TaskEntry, None]:
        logger.info("putting in queue task_data %s", task_data)

        assert depends_on is None or type(depends_on) in [list, tuple] # runtime typing!

        if depends_on is not None:
            depends_on = [ Task(dep).reference_dict for dep in depends_on ]

        task=Task(task_data, submission_data=submission_data, depends_on=depends_on)

        instances_for_key = None # type: Union[List[dqtyping.TaskEntry], None]

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

        instance_for_key = None # type: Union[dqtyping.TaskEntry, None]
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

        instance_for_key = None
        for i in range(5):
            try:
                instance_for_key = self.find_task_instances(task)[0]
                recovered_task = Task.from_task_dict(instance_for_key['task_dict_string']) # type: ignore
                break
            except Exception as e:
                logger.warning("race condition?")
                time.sleep(2.)

        if instance_for_key is None:
            raise Exception("inserted task does not exist!")


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

    def note_worker_state(self, worker_state):
        logger.debug("creating new worker state record, worker %s state: %s", self.worker_id, worker_state)
        r = EventLog.insert(
                        worker_id = self.worker_id,
                        worker_state = worker_state,
                    )\
                    .execute(database=None)
        logger.debug("create db operation result %s", r)
        
        return r
    
    def get_worker_states(self):
        return [ model_to_dict(r) for r in EventLog.select().where(EventLog.worker_state!="unset").order_by(EventLog.timestamp.desc()).limit(1).execute(database=None) ]

    def get(self, update_expected_in_s: float=-1):
        ""
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        self.note_worker_state("ready")

        max_update_expected_in_s = 3600
        default_update_expected_in_s = 1200

        if update_expected_in_s > max_update_expected_in_s:
            logger.warning("update expected timeout %s is too long, setting to maximum %s", update_expected_in_s, max_update_expected_in_s)
            update_expected_in_s = max_update_expected_in_s

        if update_expected_in_s <= 0:
            logger.warning("no update expected timeout, setting to default %s", default_update_expected_in_s)
            update_expected_in_s = default_update_expected_in_s
    
        r=TaskEntry.update({
                        TaskEntry.state:"running",
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                        TaskEntry.update_expected_in_s:update_expected_in_s
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

    def clear_event_log(self, 
                        only_older_than_days: Union[float,None]=None, 
                        only_kind: Union[str,None]=None,
                        leave_last: Union[int,None]=None):
        ""
        if only_older_than_days is None and only_kind is None and leave_last is None:
            logger.warning('this is very desctructive: clearing all event log')
            return EventLog.delete().execute(database=None)

        f = []

        if leave_last:
            i_last = EventLog.select().order_by(EventLog.id.desc()).limit(1).execute(database=None)

            if len(i_last) == 0:
                logger.warning('no events left, can not clean')
            else:
                i_last = i_last[0].id
                logger.warning('last event %d, clearing all but %s', i_last, leave_last)
                f.append(EventLog.id < i_last - leave_last)
                logger.warning('clearing older events: %s', f)

        if only_older_than_days:
            t0 = datetime.datetime.now() - datetime.timedelta(days=only_older_than_days)

            f.append(EventLog.timestamp < t0)
            logger.warning('clearing older events: %s', f)

        if only_kind is not None:
            if only_kind == "task":
                f.append(EventLog.worker_state == "unset")
                logger.warning('clearing task events: %s', f)
            elif only_kind == "worker":
                f.append(EventLog.worker_state != "unset")
                logger.warning('clearing worker events: %s', f)
            else:
                raise RuntimeError(f"unknown kind {only_kind}; expecting 'task' or 'worker'")

        if len(f) > 0:
            F = reduce(lambda x,y: x&y, f)

            N = EventLog.delete().where(F).execute(database=None)

            logger.info("clearing: %s", N)
        else:
            N = 0

        return N
    
    def clear_old_worker_events(self):
        ""
        logger.warning('this is very desctructive: clearing event log')
        EventLog.delete().execute(database=None)

    #def move_task(self, fromk: str, tok: str, task, update_entry=False, n_tries_left=1):
    def move_task(self, fromk: str, tok: str, task, update_entry=None, n_tries_left=1):
        "moves task"

        logger.info("%s moving task %s from %s to %s", self, task, fromk, tok)
        logger.info("%s moving task update entry %s", self, update_entry)

        if isinstance(task, Task):
            task_key = task.key
        else:
            task_key = task


        retry_delay=2

        if n_tries_left<=0:
            return
            
        extra = {}

        #if update_entry:
        #    extra = {TaskEntry.task_dict_string: self.current_task.serialize()}
        if update_entry is not None:
            logger.info("will update entry %s", update_entry)
            extra = {TaskEntry.task_dict_string: update_entry}

        try:
            r = TaskEntry.update({
                            TaskEntry.state:tok,
                            TaskEntry.worker_id:self.worker_id,
                            TaskEntry.modified:datetime.datetime.now(),
                            **extra
                        })\
                        .where(TaskEntry.state==fromk, TaskEntry.key==task_key).execute(database=None)

        except Exception as e:
            logger.error('failed to move task: %s', repr(e))
            #self.log_task("failed to move task from %s to %s; serialized to %i"%(repr(e),len(serialized)),state="failed_to_lock")
            self.log_task(f"failed to move task from {fromk} to {tok}: {e.__class__}:{repr(e)}; db: {db } - will try connecting after {retry_delay} s", state="failed_to_lock")

            time.sleep(retry_delay + (30 - n_tries_left))

            try:
                try:
                    db.close()
                except Exception as e:
                    self.log_task(f"db close before reconnect failed", state="db_reconnected")

                db.connect()
                self.log_task(f"managed to reconnect! {repr(e)}", state="db_reconnected")
            except peewee.OperationalError as e:
                self.log_task(f"failed to reconnect! {repr(e)}", state="failed_to_reconnect")

            return self.move_task(fromk, tok, task, update_entry, n_tries_left-1)

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




    def task_locked(self, depends_on: List[dqtyping.TaskDict]):
        ""


        if not isinstance(depends_on, list):
            raise Exception(f"depends_on has unknown type {depends_on.__class__}, expected list")

        logger.info("%s locking task %s", self, self.current_task)
        self.log_task(f"task to lock by {len(depends_on)} dependencies", state="locking")
        if self.current_task is None:
            raise Exception("task must be available to lock")

        _depends_on = [] 
        
        for dependency in depends_on:
            self.log_task(f"constructing dependency key for: {repr(dependency)[:100]}...", state="locking")
            try:
                dependency_key = Task(dependency).key
            except Exception as e:
                logger.error(f"problem constructing dependency key {e}")
                self.log_task(f"problem constructing dependency key {e}", state="locking")
                raise

            self.log_task(f"constructed dependency key: {dependency_key}", state="locking")

            #dependency_task = self.task_by_key(dependency_key)

            dependency_reference = dict(
                        task_key=dependency_key,
                    )

            _depends_on.append(dependency_reference)
        
        self.current_task.depends_on = _depends_on

        try:
            serialized=self.current_task.serialize()
        except Exception as e:
            logger.error("problem serializing task: %s", e)
            self.log_task(f"problem serializing task: {e}", state="locking")
            raise

        self.log_task("task to lock: serialized to %i"%len(serialized), state="locked")

        self.move_task('running', 'locked', 
                       self.current_task, 
                       update_entry=self.current_task.serialize(),
                       #update_entry=True, 
                       n_tries_left=30)
        
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

    def forgive_task_failures(self) -> int:
        entries = TaskEntry.select().where(TaskEntry.state=="failed").order_by(TaskEntry.modified.desc()).limit(1).execute(database=None)

        if len(entries) == 0:
            logger.info("no failed tasks: will not try to forgive")
            return 0


        entry=entries[0]

        try:
            self.current_task=Task.from_task_dict(entry.task_dict_string)
            self.current_task_stored_key=self.current_task.key
            logger.info("found %s failed tasks: will try to forgive", len(entries))
        except Exception as e:
            logger.info("will not forgive task %s with corrupt json, updating modified", entry.key)
            r=TaskEntry.update({
                        TaskEntry.modified:datetime.datetime.now(),
                        TaskEntry.state: "corrupt",
                    }).where(TaskEntry.key == entry.key).execute(database=None)
            return 0

        task = self.current_task

        history = [model_to_dict(en) for en in EventLog.select().where(EventLog.task_key == task.key).order_by(EventLog.id.desc()).execute(database=None)]
        n_failed = len([he for he in history if he['task_state'] == "failed"])

        self.log_task("task failed %i times already"%n_failed,task,"failed")
        if n_failed < n_failed_retries:
            self.log_task("task failure forgiven, to waiting",task,"waiting")
            #time.sleep( (5+2**int(n_failed/2))*sleep_multiplier )
            r=TaskEntry.update({
                        TaskEntry.state: "waiting",
                        TaskEntry.task_dict_string:self.current_task.serialize(),
                        TaskEntry.modified:datetime.datetime.now(),
                    }).where(TaskEntry.key == self.current_task.key).execute(database=None)
        else:
            self.log_task("task failure permanent",task,"waiting")

        self.current_task = None
        self.current_task_stored_key = None
        self.current_task_status = None

        return 1



    def task_failed(self,update=lambda x:None):
        update(self.current_task)

        task= self.current_task

        self.log_task("task failed",self.current_task,"failed")

        r=TaskEntry.update({
                    TaskEntry.state: "failed",
                    TaskEntry.task_dict_string:self.current_task.serialize(),
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute(database=None)

        self.current_task_status = "failed"
        self.current_task = None


    def wipe(self,wipe_from=["waiting"]):
        for fromk in wipe_from:
            for key in self.list_tasks(fromk):
                log("removing",fromk + "/" + key)
                TaskEntry.delete().where(TaskEntry.key==key).execute(database=None)

    @property
    def summary(self):
        r={}
        for kind in "waiting","running","done","failed","locked":
            r[kind] = TaskEntry.select().where(TaskEntry.state==kind, TaskEntry.queue==self.queue).count()
           # .execute(database=None)
            #
        return r
        
    @property
    def info(self):
        ""
        return self.summary

        #r = {}
        #for kind in "waiting","running","done","failed","locked":
        #    r[kind] = len(self.list_tasks(state=kind))

        #return r

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

        logger.info("log_task: %s:%s for %s at %s", task, task_key, message, state)

        return EventLog.insert(
                             queue=self.queue,
                             task_key=task_key,
                             task_state=state,
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

    def expire_tasks(self):
        # yes. all of this can be one cmmand. but we want details
        entries = TaskEntry.select().where(
                    TaskEntry.state=="running",
                ).order_by(TaskEntry.modified.desc()).limit(1).execute(database=None)

        N = 0

        for entry in entries:
            age = datetime.datetime.now().timestamp() - entry.modified.timestamp()
            expected = entry.update_expected_in_s

            logger.info("running task with age %s limit %s", age, expected)
            if age > expected:
                logger.warning("to expire key %s state %s", entry.key, entry.state)

                extra = {}

                try:
                    self.current_task=Task.from_task_dict(entry.task_dict_string)
                    self.current_task_stored_key=self.current_task.key
                    self.log_task("task failed - expired",self.current_task,"failed")
                except Exception as e:
                    logger.error("unexpected error in decoding task content: %s", repr(e))
                    extra = {TaskEntry.task_dict_string: json.dumps({'corrupt_json': entry.task_dict_string})}
                    self.log_task("task failed - corrupt json", None, "failed", task_key=entry.key)


                n = TaskEntry.update({
                            TaskEntry.state:"failed",
                            **extra
                        }).where(
                            TaskEntry.state=="running",
                            TaskEntry.key==entry.key,
                        ).execute(database=None)

                logger.warning("expired %s", n)

                N += n

        return N


