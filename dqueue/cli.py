import click
import logging
import json
import os
import pprint
import time
from termcolor import colored

logger = logging.getLogger()

log = lambda *x,**xx:logger.info(*x, **xx)

from dqueue import from_uri
from dqueue.core import Queue, Task
from dqueue.proxy import QueueProxy

@click.group()
@click.option("-d", "--debug", default=False, is_flag=True)
@click.option("-q", "--queue", default=None)
@click.pass_obj
def cli(obj, debug=False, queue=None):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if queue is None:
        queue = os.environ.get('DQUEUE_LEADER', None)
    obj['queue'] = from_uri(queue)
    logger.info("using queue: %s", obj['queue'])

@cli.command()
@click.pass_obj
def info(obj):
    for q in obj['queue'].list_queues(None):
        log(colored(q, 'green'))
        for k,v in q.info.items():
            print(k, ":", len(v), end="; ")
        print("\n")
        #log(q.list(kinds=["waiting","done","failed","running"]))
        #print(q.show())

@cli.command()
@click.pass_obj
def purge(obj):
    if True:
        obj['queue'].purge()
    else:
        for q in Queue().list_queues():
            log(q.info)
            log(q.list_task(kinds=["waiting","done","failed","running"]))
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
@click.pass_obj
def list(obj, debug, log):
    for task in obj['queue'].list_tasks():

        td = task['decoded_entry']['task_data']


        s = [colored(task['key'], "red"), task['queue'], colored(task['state'], 'blue')]

        s.append(task['created'])

        if 'object_identity' in td:
            oi = td['object_identity']

            if 'full_name' in oi:
                s.append( colored(oi['full_name'], "yellow"))
            elif 'name' in oi:
                s.append( colored(oi['name'], "yellow"))


        s.append(repr(td))


        print((" ".join(s))[:console_size()[1]])

        if debug:
            print(pprint.pformat(task))
            t = Task.from_entry(task['decoded_entry'])
            print("Task: ", t)
            print("Task key: ", t.key)

        if log:
            for l in obj['queue'].view_log(task['key'])['task_log']:
                print("  {timestamp} {message}".format(**l))

        #print('task_id', task['task_id'])
    

@cli.command()
@click.pass_obj
@click.option("--follow", "-f", is_flag=True, default=False)
def viewlog(obj, follow):
    since = 0

    waiting = False

    while True:
        new_messages = obj['queue'].view_log(since=since)['event_log']


        for l in new_messages:
            logging.debug(l)

            if waiting:
                print("")
                waiting=False

            print("{timestamp} {task_key} {message}".format(**l))

        if not follow:
            break

        if len(new_messages)==0:
            if not waiting:
                print("waiting", end="")
            else:
                print(".", end="", flush=True)

            waiting = True

        since = l['id']+1

        time.sleep(1) # nobody ever needs anything but this default

    print("")

@cli.command()
@click.option('-w', '--watch', default=None, type=int)
@click.pass_obj
def try_all_locked(obj, watch):
    while True:
        print("trying to unlock something")
        task_data=obj['queue'].try_all_locked()
        print(colored("unlocked:", "green"), task_data)
    
        if not watch:
            break

        
        print("getting queue statistics...")
        for k,v in obj['queue'].info.items():
            print(k, ":", len(v), end="; ")
        print("\n")

        print("sleeping", watch)
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
@click.pass_obj
def resubmit(obj):
    r = obj['queue'].resubmit('state', 'all').response().result
    print(colored("resubmitted:", "green"), ":", r)

def main():
    cli(obj={})

if __name__ == "__main__":
    main()

