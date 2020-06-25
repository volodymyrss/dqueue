from .core import *

from .proxy import QueueProxy

def from_uri(queue_uri):
    """"""
    if queue_uri.startswith("http://") or queue_uri.startswith("https://"):
        return QueueProxy(queue_uri)

    return Queue(queue_uri)
