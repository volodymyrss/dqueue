import click
import logging

logger = logging.getLogger()

log = lambda *x,**xx:logger.info(*x, **xx)

from dqueue.core import Queue

@click.group()
@click.option("--queue", default=None)
@click.pass_obj
def cli(obj, queue):
    obj['queue']=queue

@cli.command()
@click.pass_obj
def show(obj):
    for q in Queue.list_queues(obj['queue']):
        log(q.info)
        log(q.list(kinds=["waiting","done","failed","running"]))
        print(q.show())

@cli.command()
@click.argument("queue", default=None, required=False)
def purge(queue):
    for q in Queue.list_queues(queue):
        log(q.info)
        log(q.list(kinds=["waiting","done","failed","running"]))
        print(q.show())
        q.purge()

@cli.command()
def explore():
    from bravado.client import SwaggerClient
    client = SwaggerClient.from_url('http://localhost:8000/apispec_1.json')
    print(client)
    print(client.tasks) #.list().response().result)

    r = client.tasks.get_tasks().response().result

    print(r)

    for task in r.tasks:
        print(task)



if __name__ == "__main__":
    cli(obj={})

