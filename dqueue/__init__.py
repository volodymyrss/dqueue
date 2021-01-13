from .core import *

from .proxy import QueueProxy
from .data import DataFacts

import logging

def from_uri(queue_uri: Union[str, None]=None, worker_id: Union[str, None]=None):
    """"""
    logger = logging.getLogger("from_uri")

    local, remote = Queue, QueueProxy

    if queue_uri is None:
        if 'ODAHUB' in os.environ:
            queue_uri = os.environ['ODAHUB']
            logger.info("getting ODAHUB from env: %s", queue_uri)
        else:
            queue_uri = "https://crux.staging-1-3.odahub.io@default"
            logger.info("using hard-coded ODAHUB point: %s", queue_uri)

    for uri in queue_uri.split(","):
        logger.info("found ODAHUB URI option: %s", uri)

        if uri.startswith("http://") or uri.startswith("https://"):
            r = remote(uri, worker_id)
        else:
            r = local(uri, worker_id)

        try:
            logger.info("probing connection...")
            r.version()
            logger.info("succeeded!")
            logger.debug("constructed queue from %s: %s", uri, r)
            return r
        except Exception as e: #todo
            logger.warning("ODAHUB option %s unavailable: %s", uri, e)

    raise Exception(f"failed to find ODAHUB, tried: \"{queue_uri}\"")

