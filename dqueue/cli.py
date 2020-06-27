import click
import logging
import json
import os
import pprint
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
def cli(obj, debug, queue):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if queue is None:
        queue = os.environ.get('DQUEUE_MASTER', None)
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
        for q in Queue.list_queues(queue):
            log(q.info)
            log(q.list(kinds=["waiting","done","failed","running"]))
            q.purge()

@cli.command()
@click.option("-d", "--debug", default=False, is_flag=True)
@click.pass_obj
def list(obj, debug):
    for task in obj['queue'].list():
        print(colored("found", "red"), task['queue'], colored(task['state'], 'blue'), task['entry']['task_data'])
        if debug:
            print(pprint.pformat(task))
        #print('task_id', task['task_id'])
    

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

