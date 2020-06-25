import click
import logging
from termcolor import colored

logger = logging.getLogger()

log = lambda *x,**xx:logger.info(*x, **xx)

from dqueue import from_uri
from dqueue.core import Queue
from dqueue.proxy import QueueProxy

@click.group()
@click.option("--queue", default=None)
@click.pass_obj
def cli(obj, queue):
    obj['queue'] = from_uri(queue)

@cli.command()
@click.pass_obj
def show(obj):
    for q in Queue.list_queues(obj['queue']):
        log(q.info)
        log(q.list(kinds=["waiting","done","failed","running"]))
        print(q.show())

@cli.command()
@click.pass_obj
def purge(obj):
    if True:
        obj['queue'].purge()
    else:
        for q in Queue.list_queues(queue):
            log(q.info)
            log(q.list(kinds=["waiting","done","failed","running"]))
            print(q.show())
            q.purge()

@cli.command()
@click.pass_obj
def list(obj):
    for task in obj['queue'].list():
        print(colored("found", "red"), task)
        print('task_id', task['task_id'])
    

def main():
    cli(obj={})

if __name__ == "__main__":
    main()

