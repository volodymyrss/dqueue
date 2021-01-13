import click
import logging
import requests
import json
import io
import os
import sys
import yaml
import pprint
import time
import subprocess
import datetime
import random
import socket
from termcolor import colored
from collections import defaultdict
import pylogstash

from dqueue import from_uri
from dqueue.core import Queue, Task
from dqueue.proxy import QueueProxy

import dqueue.core as core 

import coloredlogs

logger = logging.getLogger()

log_stasher = pylogstash.LogStasher(sep="/")

@click.group()
@click.option("-q", "--quiet", default=False, is_flag=True)
@click.option("-d", "--debug", default=False, is_flag=True)
@click.option("-q", "--queue", default=None)
@click.pass_obj
def cli(obj, quiet=False, debug=False, queue=None):
    if quiet:
        level = logging.CRITICAL
    else:
        if debug:
            level=logging.DEBUG
        else:
            level=logging.INFO

    logging.basicConfig(level=level)
    coloredlogs.install(level=level)

    obj['queue'] = from_uri(queue)
    logger.debug("using queue: %s", obj['queue'])

@cli.command()
@click.option("-v", "--validate", default=False, is_flag=True)
@click.pass_obj
def version(obj, validate):
    hub = obj['queue'].version()
    print("client:", core.__version__)
    print("hub:", hub['version'])

    if validate:
        if core.__version__ == hub['version']:
            logger.info("versions compatible!")
        else:
            logger.error("versions INcompatible!")
            sys.exit(1)

@cli.command()
@click.pass_context
def hub_info(ctx):
    ctx.invoke(version, validate=False)

@cli.command()
@click.pass_obj
def auth(obj):
    print(core.__version__)
    print("my auth level: ADMIN")


def log_info(queue):
    for q in queue.list_queues(None):
        print(f"\033[37m{q}\033[0m")
        print(f"\033[1;31m{'live facts':>20s}: \033[0m\033[1;36m", "; ".join(f"{k}: {v}" for k,v in q.summary.items()), "\033[0m")

@cli.command()
@click.pass_obj
def info(obj):
    log_info(obj['queue'])

@cli.command()
@click.pass_obj
def purge(obj):
    if True:
        obj['queue'].purge()
    else:
        for q in Queue().list_queues():
            logger.info(q.info)
            logger.info(q.list_task(kinds=["waiting","done","failed","running"]))
            q.purge()

def console_size():
    try:
        return [int(i) for i in os.popen('stty size', 'r').read().split()]
    except Exception as e:
        logger.warning("problem getting console size: %s", e)
        return 40, 200

@cli.command("ls")
@click.option("-d", "--debug", default=False, is_flag=True)
@click.option("-l", "--log", default=False, is_flag=True)
@click.option("-i", "--info", default=False, is_flag=True)
@click.option("-s", "--select", default=None)
@click.option("-j", "--json-output", default=None)
#@click.option("-a", "--max-age", default=None)
@click.pass_obj
def list(obj, debug, log, info, select, json_output):
    state = None
    select_task = None

    j_f = None
    if json_output:
        j_f = open(json_output, "wt")


    if select is not None:
        selector, selection = select.split(":")

        if selector == "state":
            state = selection
        elif selector == "task":
            select_task = selection

    for task in obj['queue'].list_tasks(state=state):
        if select_task is not None:
            if task['key'] != select_task:
                continue

        td = task['task_dict']['task_data']


        s = [colored(task['key'], "red"), task['queue'], colored(task['state'], 'blue')]

        s.append(task['created'])

        if 'object_identity' in td:
            oi = td['object_identity']

            if 'full_name' in oi:
                s.append( colored(oi['full_name'], "yellow"))
            elif 'name' in oi:
                s.append( colored(oi['name'], "yellow"))

        if info:
            ti = obj['queue'].task_info(task['key'])
            print(ti['task_info']['task_data']['object_identity']['factory_name'] )#['task_dict'])


        s.append(repr(td))


        cut_width = console_size()[1]
        m = " ".join(s)

        print(m[:cut_width], end="")
        if len(m) > cut_width:
            print(colored(f"...{len(m)-cut_width} more", "blue"))
        else:
            print("")

        if debug:
            print(json.dumps(task, indent=4,sort_keys=True))
            t = Task.from_task_dict(task['task_dict'])
            print("Task: ", t)
            print("Task key: ", t.key)
        
        if j_f is not None:
            json.dump(task['task_dict'], j_f)

        if log:
            for l in obj['queue'].view_log(task['key'])['event_log']:
                print("  {timestamp} {message}".format(**l))

        #print('task_id', task['task_id'])
    
@cli.command()
@click.pass_obj
@click.argument("key")
@click.option("--follow", "-f", is_flag=True, default=False)
def viewtask(obj, key, follow):
    ti = obj['queue'].task_info(key)

    print(json.dumps(ti))

@cli.group("log")
def logcli():
    pass

@logcli.command()
@click.pass_obj
@click.option("--follow", "-f", is_flag=True, default=False)
@click.option("--since", "-s", default=0)
@click.option("--window", "-w", default=60)
def view(obj, follow, since=0, window=60):
    waiting = False

    info_cadence = 10
    till_next_info = info_cadence
    last_info_time = 0

    task_info_cache={}

    active_workers = defaultdict(dict)

    while True:
        new_messages = obj['queue'].view_log(since=since)['event_log']


        for l in new_messages:
            logging.debug(l)

            l['timestamp_seconds'] = time.mktime(time.strptime(l["timestamp"], "%a, %d %b %Y %H:%M:%S %Z"))

            if waiting:
                print("\n")
                waiting=False


            if l['task_key'] not in task_info_cache:
                ti = obj['queue'].task_by_key(l['task_key'], decode=True)
                task_info_cache[l['task_key']] = ti


            ti = task_info_cache[l['task_key']]

            logger.debug(ti)

            name = None
            if ti is not None:
                try:
                    name = ti['task_dict']['task_data']['object_identity']['factory_name']
                except Exception as e:
                    logger.error("very stange task: %s; %s", ti.keys(), e)
                    name = "missing"

            if name is None or name == "??": # ???
                try:
                    m = json.loads(l['message'])
                    name = m['params']['node']
                    l['message'] = f"{m['qs']['job_id'][0][:8]} {m['params']['message']}"
                except Exception as e:
                    logger.debug("%s", repr(e))

            print(("{since} {timestamp} "+colored("{task_key:10s}", "red") + " {message:40s} "+colored("{name:20s}", "yellow") + colored(" {worker_id:40s}", "cyan") ).format(
                    since=since,
                    name=name,
                    **l))

            w = active_workers[l['worker_id']]
            w['last_active_timestamp'] = l["timestamp_seconds"]
            if l['message'] == "task done":
                w['stats_done'] = w.get('stats_done', 0) + 1
            w['stats_all'] = w.get('stats_all', 0) + 1
            w['last_message'] = l['message']
            w['last_name'] = name
            w['last_task_key'] = l['task_key']

            logger.debug(l)
            

            since = l['id']+1

        if not follow:
            break

        if len(new_messages)==0:
            if not waiting:
                print("waiting", end="")
            else:
                print(".", end="", flush=True)

            waiting = True

            till_next_info -= 1

            if till_next_info <=0:
                till_next_info = info_cadence

        if time.time() - last_info_time > 5:

            log_info(obj['queue'])

            recent_workers = [k for k, v in active_workers.items() if
                                    v['last_active_timestamp']>time.mktime(time.gmtime())-window and not k.startswith('oda-dqueue-')]
            print(f"\033[1;31m{len(recent_workers):>5d} recent workers:\033[0m \033[1;35m{', '.join(recent_workers)}\033[0m")

            for k,v in active_workers.items():
                if k.startswith('oda-dqueue-'): continue
                if not v['last_active_timestamp']>time.mktime(time.gmtime())-window: continue

                print(f"\033[37m- {time.mktime(time.gmtime())-v['last_active_timestamp']:4.0f}s ago {k[:30]:30}" + 
                      f" {v.get('stats_done', 0):3d} / {v.get('stats_all', 0):4d}" +
                      f" {v['last_message']:40s} {v['last_name']:20s} {v['last_task_key']:10s}\033[0m")

            last_info_time = time.time()


        time.sleep(1) # nobody ever needs anything but this default

    print("")

@logcli.command()
@click.pass_obj
@click.option("--before", "-b", default=None, type=float)
@click.option("--kind", "-k", default=None, type=str)
def clear(obj, before, kind):
     N = obj['queue'].clear_event_log(before, kind)
     print("cleared", N)

###

@cli.group("data")
def datacli():
    pass

@datacli.command("assert")
@click.pass_obj
@click.option("-g", "--dag", type=click.File('rt'))
@click.option("-d", "--data", type=click.File('rt'))
def assert_fact(obj, dag, data):
    dag = json.load(dag)
    data = json.load(data)

    r = obj['queue'].assert_fact(
                dag=dag,
                data=data,
            )

    print("assert_fact returns", r)

@datacli.command()
@click.pass_obj
@click.option("-g", "--dag", type=click.File('rt'))
@click.option("-o", "--output", type=click.File('wt'))
def consult(obj, dag, output):
    dag = json.load(dag)

    d = obj['queue'].consult_fact(dag=dag)

    json.dump(d, output)

    

@datacli.command()
@click.pass_obj
def list_facts(obj):
    pass

###

@cli.command()
@click.option('-w', '--watch', default=None, type=int)
@click.pass_obj
def guardian(obj, watch):
    while True:
        #expre
        print("exiure some tasks")
        r = obj['queue'].expire_tasks()
        print(colored("expired:", "yellow"), r)

        #try_all_locked
        print("trying to unlock something")
        task_data=obj['queue'].try_all_locked()
        print(colored("unlocked:", "green"), task_data)
        
        #try_all_locked
        print("trying to forgive failures")
        task_data=obj['queue'].forgive_task_failures()
        print(colored("forgiven:", "green"), task_data)
        
        #clear event log 
        N = obj['queue'].clear_event_log(leave_last=100)
        #N = obj['queue'].clear_event_log(only_older_than_days=2./24.)
        print(f"cleared event log of {N} entries")

        # stats

        print("getting queue statistics...")
        print(">>", obj['queue'].get_summary())
        log_info(obj['queue'])
        print("\n")

        log_stasher.log(
                    dict(
                        origin="oda-node",
                        action="queue-status",
                        summary=obj['queue'].summary,
                    )
                )
        
        for k,v in obj['queue'].summary.items():
            log_stasher.log(
                         { 
                            "origin": "oda-node",
                            "action": "queue-status-per-kind",
                            "oda-node/queue/kind": k,
                            "oda-node/queue/njobs": v,
                        } 
                    )

        print("sleeping", watch)

        if not watch:
            break
        time.sleep(watch)

@cli.command()
@click.pass_obj
def get(obj):
    task_data=obj['queue'].get()
    print(colored("offered:", "green"), task_data)

@cli.command()
@click.argument("task_data")
@click.pass_obj
def question(obj, task_data):
    j_task_data=json.loads(task_data)
    r = obj['queue'].put(j_task_data)
    print(colored("questioned:", "green"), task_data, ":", r)

@cli.command()
@click.option('-s', '--scope-selector', default="state:all")
@click.pass_obj
def delete(obj, scope_selector):
    scope, selector = scope_selector.split(":")

    log_info(obj['queue'])
    r = obj['queue'].delete(scope, selector)
    print(colored("deleted:", "green"), ":", r)
    log_info(obj['queue'])

@cli.command()
@click.option('-s', '--scope-selector', default="state:all")
@click.pass_obj
def resubmit(obj, scope_selector):
    scope, selector = scope_selector.split(":")

    log_info(obj['queue'])
    r = obj['queue'].resubmit(scope, selector)
    print(colored("resubmitted:", "green"), ":", r)
    log_info(obj['queue'])

#####
@cli.group("runner")
def runnercli():
    pass

@runnercli.command()
@click.option("-d", "--deploy-runner-command", default=None)
@click.option("-l", "--list-runners-command", default=None)
@click.option("-p", "--profile", default=None)
@click.option("-t", "--timeout", default=10)
@click.option("-m", "--max-runners", default=100)
@click.option("-n", "--min-waiting-jobs", default=2)
@click.pass_obj
def start_executor(obj, deploy_runner_command, list_runners_command, profile, timeout, max_runners, min_waiting_jobs):
    if profile is not None:
        if profile.startswith(":"):
            p = yaml.safe_load(io.BytesIO(requests.get("https://raw.githubusercontent.com/volodymyrss/oda-runner-profiles/main/{}.yaml".format(profile[1:])).content)) # long and hard-coded string
        else:
            if os.path.exists(profile):
                p = yaml.safe_load(open(profile))
            else:
                logger.error("profile file %s does not exist!", profile)
                sys.exit(1)

        deploy_runner_command = p['deploy_runner_command']
        list_runners_command = p['list_runners_command']


    if deploy_runner_command is None:
        logger.error("executor needs deploy_runner_command, either in argument or in profile")
        sys.exit(1)
    
    if list_runners_command is None:
        logger.error("executor needs list_runners_command, either in argument or in profile")
        sys.exit(1)

    age = 0

    while True:
        r = obj['queue'].list_queues(None)
        for q in r:
            summary = q.summary
            print(f"queue: \033[33m{q}\033[0m", "; ".join([ f"{k}: {v}" for k, v in summary.items() ]))

            tpars = dict(
                dt=datetime.datetime.now(),
                randint=random.randint(0, 100000),
                qsummary=summary,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                age=age,
            )

            if summary['waiting'] > 0:
                print(f"\033[31mfound {summary['waiting']} waiting jobs, need to start some runners\033[0m")
                
                runners = subprocess.check_output(["bash", "-c", list_runners_command]).decode().split("\n")

                if len(runners) == 1 and runners[0] == "":
                    runners = []
                
                print(f"\033[33mfound {len(runners)} live runners, max {max_runners}, min waiting to trigger {min_waiting_jobs}, need at least {summary['waiting'] - min_waiting_jobs}\033[0m")
                if len(runners) >= min(max_runners, summary['waiting'] - min_waiting_jobs):
                    print(f"\033[33mfound enough runners {len(runners)}\033[0m")
                else:
                    cmd = deploy_runner_command.format(**tpars)
                    print(f"\033[31mexecuting: {cmd}\033[0m")
                    subprocess.check_output(["bash", "-c", cmd])

            age += 1


        time.sleep(timeout)

@runnercli.command()
@click.pass_obj
def execute(obj):
    print("""
    the executor is specific to a workflow. in general, executor will use oda-node package to fetch tasks
    an example runner for INTEGRAL pipeline is available with data-analysis  module and \033[32moda-runner-execute\033[0m command
    """)


@cli.command()
@click.argument("target")
@click.option("-m", "--module", multiple=True)
@click.option("-a", "--assume", multiple=True)
@click.pass_obj
def ask(obj, target, module, assume):
    task_data = dict(
                object_identity=dict(
                    assumptions=[
                            [target, {'request_root_node': True}]
                        ] + [['', a] for a in assume],
                    expected_hashe='None',
                    factory_name=target,
                    full_name=target,
                    modules=[
                            ['git', m[len("git://"):].split("/")[0], m]
                            for m in module
                        ]
                )
            )

    r = obj['queue'].put(
            task_data,
            submission_data=dict(
                callbacks=[],
                request_origin="cli",
                ),
            )

    #print("odahub responds", r)
    print(f"\033[31m{r['state']:10s}\033[0m \033[33m{r['modified']}\033[0m \033[34m{r['created']}\033[0m")


def main():
    cli(obj={})

if __name__ == "__main__":
    main()

