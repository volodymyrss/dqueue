from __future__ import print_function, division

import yaml
import datetime
import os
import time
import socket
from hashlib import sha224
from collections import OrderedDict
import glob
import logging
import StringIO

import urlparse

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler=logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
handler.setFormatter(formatter)

def log(*args,**kwargs):
    severity=kwargs.get('severity','info').upper()
    logger.log(getattr(logging,severity)," ".join([repr(arg) for arg in list(args)+list(kwargs.items())]))


class Empty(Exception):
    pass

class CurrentTaskUnfinished(Exception):
    pass

class TaskStolen(Exception):
    pass

import pymysql
import peewee
from playhouse.db_url import connect
from playhouse.shortcuts import model_to_dict, dict_to_model

db=connect(os.environ.get("DQUEUE_DATABASE_URL"))
#db.get_conn().ping(True)

class TaskEntry(peewee.Model):
    queue = peewee.CharField(default="default")

    key = peewee.CharField(primary_key=True)
    state = peewee.CharField()
    worker_id = peewee.CharField()
    entry = peewee.TextField()

    created = peewee.DateTimeField()
    modified = peewee.DateTimeField()

    class Meta:
        database = db

class TaskHistory(peewee.Model):
    queue = peewee.CharField(default="default")

    key = peewee.CharField()
    state = peewee.CharField()
    worker_id = peewee.CharField()

    timestamp = peewee.DateTimeField()
    message = peewee.CharField()

    class Meta:
        database = db

db.create_tables([TaskEntry,TaskHistory])

class Task(object):
    def __init__(self,task_data,execution_info=None, submission_data=None, depends_on=None):
        self.task_data=task_data
        self.submission_info=self.construct_submission_info()
        self.depends_on=depends_on

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

    def serialize(self):
        return yaml.dump(dict(
                submission_info=self.submission_info,
                task_data=self.task_data,
                execution_info=self.execution_info,
                depends_on=self.depends_on,
            ),
            default_flow_style=False, default_style=''
        )


    @classmethod
    def from_entry(cls,entry):
        task_dict=yaml.load(StringIO.StringIO(entry))

        self=cls(task_dict['task_data'])
        self.depends_on=task_dict['depends_on']
        self.submission_info=task_dict['submission_info']

        return self

        


    @property
    def key(self):
        return self.get_key(True)

    def get_key(self,key=True):
        components=[]

        task_data_string=yaml.dump(self.task_data,encoding='utf-8')

        components.append(sha224(task_data_string).hexdigest()[:8])
        log("encoding: "+repr(components),severity="debug")
        log(task_data_string,severity="debug")

        if not key:
            components.append("%.14lg"%self.submission_info['time'])
            components.append(self.submission_info['utc'])

            components.append(sha224(str(OrderedDict(sorted(self.submission_info.items()))).encode('utf-8')).hexdigest()[:8])

        return "_".join(components)

    def __repr__(self):
        return "[{}: {}]".format(self.__class__.__name__,self.task_data)

def makedir_if_neccessary(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != 17: raise


class Queue(object):

    def __init__(self,queue="default"):
        self.worker_id=self.get_worker_id()
        self.queue=queue
        self.current_task=None
        self.current_task_status=None

    def get_worker_id(self):
        d=dict(
            time=time.time(),
            utc=time.strftime("%Y%m%d-%H%M%S"),
            hostname=socket.gethostname(),
            fqdn=socket.getfqdn(),
            pid=os.getpid(),
        )
        return "{fqdn}.{pid}".format(**d)


    def find_task_instances(self,task,klist=None):
        if klist is None:
            klist=["waiting", "running", "done", "failed", "locked"]

        instances_for_key = []
        for state in klist:
            instances_for_key+=[
                    dict(state=state,task_entry=task_entry) for task_entry in TaskEntry.select().where(TaskEntry.state==state, TaskEntry.key==task.key, TaskEntry.queue==self.queue)
                ]
        return instances_for_key
    
    def try_all_locked(self):
        r=[]
        for task_key in self.list("locked"):
            task_entry=self.select_task_entry(task_key)
            log("trying to unlock", task_entry.key)
            #log("trying to unlock", task_key,task_entry,task_entry.key,task_entry.entry)
            r.append(self.try_to_unlock(Task.from_entry(task_entry.entry)))
        return r

    def try_to_unlock(self,task):
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
    
    def remember(self,task_data,submission_data=None):
        task=Task(task_data,submission_data=submission_data)
        nfn=self.queue_dir("problem") + "/"+task.filename_instance
        open(nfn, "w").write(task.serialize())
            
    
    def select_task_entry(self,key):
        r=TaskEntry.select().where(
                         TaskEntry.key==key,
                        ).execute()
        assert len(r)==1
        return r[0]
    
    def log_task(self,message,task=None,state=None):
        if task is None:
            task=self.current_task
        if state is None:
            state=self.current_task_status
        return TaskHistory.insert(
                             queue=self.queue,
                             key=task.key,
                             state=state,
                             worker_id=self.worker_id,
                             timestamp=datetime.datetime.now(),
                             message=message,
                        ).execute()

    def insert_task_entry(self,task,state):
        self.log_task("task created",task,state)

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
        except pymysql.err.IntegrityError as e:
            log("task already inserted")

    def put(self,task_data,submission_data=None, depends_on=None):
        assert depends_on is None or type(depends_on) in [list,tuple]

        task=Task(task_data,submission_data=submission_data,depends_on=depends_on)

        ntry_race=10
        while True:
            instances_for_key=self.find_task_instances(task)
            if len(instances_for_key)<=1:
                break
            time.sleep(1)
            ntry_race-=1
        if len(instances_for_key)>1:
            raise Exception("probably race condition, multiple task instances:",instances_for_key)


        if len(instances_for_key) == 1:
            instance_for_key=instances_for_key[0]
        else:
            instance_for_key=None

        if instance_for_key is not None:
            log("found existing instance(s) for this key, no need to put:",instances_for_key)
            self.log_task("task already found",task,instance_for_key['state'])
            return instance_for_key

        if depends_on is None:
            self.insert_task_entry(task,"waiting")
        else:
            self.insert_task_entry(task,"locked")

        instance_for_key=self.find_task_instances(task)[0]
        recovered_task=Task.from_entry(instance_for_key['task_entry'].entry)

        if recovered_task.key != task.key:
            log("inconsitent storage:")
            log("stored:",task.filename_instance)
            log("recovered:", recovered_task.filename_instance)
    
            nfn=self.queue_dir("conflict") + "/put_original_" + task.filename_instance
            open(nfn, "w").write(task.serialize())
        
            nfn=self.queue_dir("conflict") + "/put_recovered_" + recovered_task.filename_instance
            open(nfn, "w").write(recovered_task.serialize())
            
            nfn=self.queue_dir("conflict") + "/put_stored_" + os.path.basename(fn)
            open(nfn, "w").write(open(fn).read())

            raise Exception("Inconsistent storage")

        log("successfully put in queue:",instance_for_key['task_entry'].entry)
        return dict(state="submitted",task_entry=instance_for_key['task_entry'].entry)

    def get(self):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

       # tasks=self.list("waiting")
       # task=tasks[-1]
       # self.current_task = Task.from_entry(task['task_entry'].entry)

    
        r=TaskEntry.update({
                        TaskEntry.state:"running",
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .order_by(TaskEntry.created.desc())\
                    .where(TaskEntry.state=="waiting").limit(1).execute()

        if r==0:
            self.try_all_locked()
            raise Empty()

        entries=TaskEntry.select().where(TaskEntry.worker_id==self.worker_id,TaskEntry.state=="running").order_by(TaskEntry.modified.desc()).limit(1).execute()
        if len(entries)>1:
            raise Exception("what?")

        entry=entries[0]
        self.current_task=Task.from_entry(entry.entry)

        log(self.current_task.key)
        

        if self.current_task.key != entry.key:
            log("inconsitent storage:")
            log(">>>> stored:", task_name)
            log(">>>> recovered:", self.current_task.filename_instance)

            fn=self.queue_dir("conflict") + "/get_stored_" + self.current_task.filename_instance
            open(fn, "w").write(self.current_task.serialize())
        
            fn=self.queue_dir("conflict") + "/get_recovered_" + task_name
            open(fn, "w").write(open(self.queue_dir("waiting")+"/"+task_name).read())

            raise Exception("Inconsistent storage")


        log("task is running",self.current_task)
        self.current_task_status = "running"

        self.log_task("task started")

        log('task',self.current_task.submission_info)

        return self.current_task

    def find_dependecies_states(self,task):
        if task.depends_on is None:
            raise Exception("can not inspect dependecies in an independent task!")

        dependencies=[]
        for dependency in task.depends_on:
            dependency_task=Task(dependency)
            dependency_instances=self.find_task_instances(dependency_task)

            dependencies.append(dict(states=[]))

            for i in dependency_instances:
                # if i['state']=="done"]) == 0:
                #log("dependency incomplete")
                dependencies[-1]['states'].append(i['state'])
                dependencies[-1]['task']=dependency_task

            if 'done' in dependencies[-1]['states']:
                dependencies[-1]['state']='done'
            elif 'failed' in dependencies[-1]['states']:
                dependencies[-1]['state']='failed'
            else:
                dependencies[-1]['state']='incomplete'
            
            log("dependency:",dependencies[-1]['state'],dependencies[-1]['states'], dependencies[-1]['task'].key, dependency_instances[0])
            #log("dependency:",dependencies[-1]['state'],dependencies[-1]['states'], dependency, dependency_instances)

        return dependencies




    def task_locked(self,depends_on):
        log("locking task",self.current_task)
        self.log_task("task to lock...",state="locked")
        if self.current_task is None:
            raise Exception("task must be available to lock")

        self.current_task.depends_on=depends_on
        serialized=self.current_task.serialize()

        self.log_task("task to lock: serialized to %i"%len(serialized),state="locked")

        n_tries_left=10
        retry_delay=2
        while n_tries_left>0:
            try:
                r=TaskEntry.update({
                            TaskEntry.state:"locked",
                            TaskEntry.entry:serialized,
                        }).where(
                            TaskEntry.key==self.current_task.key,
                            TaskEntry.state=="running",
                         ).execute()
            except Exception as e:
                log('failed to lock:',repr(e))
                self.log_task("task to failed lock: %s; serialized to %i"%(repr(e),len(serialized)),state="failed_to_lock")
                time.sleep(retry_delay)
                if n_tries_left==1:
                    raise
                n_tries_left-=1
            else:
                break
        
        self.log_task("task locked from "+self.current_task_status,state="locked")

        self.current_task_status="locked"
        self.current_task=None


    def task_done(self):
        log("task done",self.current_task)

        self.log_task("task to register done")

        r=TaskEntry.update({
                    TaskEntry.state:"done",
                    TaskEntry.entry:self.current_task.serialize(),
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute()

        self.current_task_status="done"

        self.log_task("task done")
        log('task done')

        self.current_task=None

    def task_failed(self,update=lambda x:None):
        update(self.current_task)

        r=TaskEntry.update({
                    TaskEntry.state:"failed",
                    TaskEntry.entry:self.current_task.serialize(),
                    TaskEntry.modified:datetime.datetime.now(),
                }).where(TaskEntry.key==self.current_task.key).execute()

        self.current_task_status = "failed"
        self.current_task = None

    def move_task(self,fromk,tok,task):
        r=TaskEntry.update({
                        TaskEntry.state:tok,
                        TaskEntry.worker_id:self.worker_id,
                        TaskEntry.modified:datetime.datetime.now(),
                    })\
                    .where(TaskEntry.state==fromk,TaskEntry.key==task.key).execute()

    def remove_task(self,fromk,taskname=None):
        pass

    def wipe(self,wipe_from=["waiting"]):
        for fromk in wipe_from:
            for key in self.list(fromk):
                log("removing",fromk + "/" + key)
                TaskEntry.delete().where(TaskEntry.key==key).execute()

    def list(self,kind=None,kinds=None,fullpath=False):
        if kinds is None:
            kinds=["waiting"]
        if kind is not None:
            kinds=[kind]

        kind_jobs = []

        for kind in kinds:
            for task_entry in TaskEntry.select().where(TaskEntry.state==kind):
                kind_jobs.append(task_entry.key)
        return kind_jobs

    @property
    def info(self):
        r={}
        for kind in "waiting","running","done","failed","locked":
            r[kind]=len(self.list(kind))
        return r

    def show(self):
        r=""
        for kind in "waiting","running","done","failed","locked":
            r+="\n= "+kind+"\n"
            for task_entry in TaskEntry.select().where(TaskEntry.state==kind):
                r+=" - "+repr(model_to_dict(task_entry))+"\n"
        return r

    def watch(self,delay=1):
        while True:
            log(self.info())
            time.sleep(delay)



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("queue",default="./queue")
    parser.add_argument('-L', dest='listen',  help='...',action='store_true', default=False)
    parser.add_argument('-H', dest='host',  help='...', default="0.0.0.0")
    args=parser.parse_args()

    if args.listen:
        from flask import Flask
        from flask import render_template,make_response

        app = Flask(__name__)

        decoded_entries={}

        @app.route('/')
        def list():
            try:
                db.connect()
            except peewee.OperationalError as e:
                pass

            print("searching for entries")
            entries=[model_to_dict(entry) for entry in TaskEntry.select().order_by(TaskEntry.modified.desc()).execute()]
            print("found entries",len(entries))
            for entry in entries:
                print("decoding",len(entry['entry']))
                if entry['entry'] in decoded_entries:
                    entry_data=decoded_entries[entry['entry']]
                else:
                    try:
                        entry_data=yaml.load(StringIO.StringIO(entry['entry']))
                        entry_data['submission_info']['callback_parameters']={}
                        for callback in entry_data['submission_info']['callbacks']:
                            if callback is not None:
                                entry_data['submission_info']['callback_parameters'].update(urlparse.parse_qs(callback.split("?",1)[1]))
                            else:
                                entry_data['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))
                    except:
                        entry_data={'task_data':
                                        {'object_identity':
                                            {'factory_name':'??'}},
                                    'submission_info':
                                        {'callback_parameters':
                                            {'job_id':['??'],
                                             'session_id':['??']}}
                                    }


                    decoded_entries[entry['entry']]=entry_data
                entry['entry']=entry_data

            db.close()
            return render_template('task_list.html', entries=entries)
        
        @app.route('/task/info/<string:key>')
        def task_info(key):
            entry=[model_to_dict(entry) for entry in TaskEntry.select().where(TaskEntry.key==key).execute()]
            if len(entry)==0:
                return make_response("no such entry found")

            entry=entry[0]

            print("decoding",len(entry['entry']))

            try:
                entry_data=yaml.load(StringIO.StringIO(entry['entry']))
                entry['entry']=entry_data
                    
                from ansi2html import ansi2html

                if entry['entry']['execution_info'] is not None:
                    entry['exception']=entry['entry']['execution_info']['exception']['exception_message']
                    formatted_exception=ansi2html(entry['entry']['execution_info']['exception']['formatted_exception']).split("\n")
                else:
                    entry['exception']="no exception"
                    formatted_exception=["no exception"]
                
                history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==key).order_by(TaskHistory.id.desc()).execute()]
                #history=[model_to_dict(en) for en in TaskHistory.select().where(TaskHistory.key==key).order_by(TaskHistory.timestamp.desc()).execute()]

                return render_template('task_info.html', entry=entry,history=history,formatted_exception=formatted_exception)
            except:
                return entry['entry']
        
        @app.route('/purge')
        def purge():
            nentries=TaskEntry.delete().execute()
            return make_response("deleted %i"%nentries)

        @app.route('/resubmit/<string:scope>/<string:selector>')
        def resubmit(scope,selector):
            if scope=="state":
                if selector=="all":
                    nentries=TaskEntry.update({
                                    TaskEntry.state:"waiting",
                                    TaskEntry.modified:datetime.datetime.now(),
                                })\
                                .execute()
                else:
                    nentries=TaskEntry.update({
                                    TaskEntry.state:"waiting",
                                    TaskEntry.modified:datetime.datetime.now(),
                                })\
                                .where(TaskEntry.state==selector)\
                                .execute()
            elif scope=="task":
                nentries=TaskEntry.update({
                                TaskEntry.state:"waiting",
                                TaskEntry.modified:datetime.datetime.now(),
                            })\
                            .where(TaskEntry.key==selector)\
                            .execute()

            return make_response("resubmitted %i"%nentries)


#         class MyApplication(Application):
#            def load(self):

#                  from openlibrary.coverstore import server, code
#                   server.load_config(configfile)
#                    return code.app.wsgifunc()

        #from gunicorn.app.base import Application
        #Application().run(app,port=5555,debug=True,host=args.host)

 #       MyApplication().run()
        app.run(port=5555,debug=True,host=args.host,threaded=True)
    else:
        log(Queue().info)
        log(Queue().list(kinds=["waiting","done","failed","running"]))
        print(Queue().show())
