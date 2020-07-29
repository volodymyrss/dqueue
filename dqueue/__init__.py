from .core import *

from .proxy import QueueProxy

import logging

def from_uri(queue_uri):
    """"""
    logger = logging.getLogger("from_uri")


    if queue_uri.startswith("http://") or queue_uri.startswith("https://"):
        r = QueueProxy(queue_uri)
    else:
        r = Queue(queue_uri)

    logger.info("constructing queue from %s: %s", queue_uri, r)
    return r
