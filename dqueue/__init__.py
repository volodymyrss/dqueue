from .core import *

from .proxy import QueueProxy
from .data import DataFacts

import logging

def from_uri(queue_uri: Union[str, None]=None):
    """"""
    logger = logging.getLogger("from_uri")

    local, remote = Queue, QueueProxy

    if queue_uri is None:
        if 'ODAHUB' in os.environ:
            queue_uri = os.environ['ODAHUB']
            logger.info("getting ODAHUB from env: %s", queue_uri)

    if queue_uri.startswith("http://") or queue_uri.startswith("https://"):
        r = remote(queue_uri)
    else:
        r = local(queue_uri)

    logger.info("constructing queue from %s: %s", queue_uri, r)
    return r
