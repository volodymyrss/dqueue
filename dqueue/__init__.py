from .core import *

from .proxy import QueueProxy

import logging

def from_uri(queue_uri: str, kind: str="queue"):
    """"""
    logger = logging.getLogger("from_uri")

    if kind == "queue":
        local, remote = Queue, QueueProxy
    elif kind == "data":
        local, remote = None, DataFacts
    else:
        raise RuntimeError(f"undefined kind: {kind}")

    if queue_uri.startswith("http://") or queue_uri.startswith("https://"):
        r = remote(queue_uri)
    else:
        r = local(queue_uri)

    logger.info("constructing queue from %s: %s", queue_uri, r)
    return r
