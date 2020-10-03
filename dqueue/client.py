import datetime
import os
import time
import socket
from hashlib import sha224
from collections import defaultdict
import glob
import logging
from io import StringIO
import re
import click
from urllib.parse import urlparse# type: ignore

from dqueue.core import Queue, Empty, Task, CurrentTaskUnfinished
import dqueue.core as core
from typing import Union
from dqueue import tools

from retrying import retry # type: ignore

from bravado.client import SwaggerClient, RequestsClient

logger = logging.getLogger(__name__) 

class APIClient:
    leader = None
    queue = None

    _token = None
    worker_id = None

    def __repr__(self):
        return f"[ {self.__class__.__name__}: leader={self.leader} queue={self.queue} ]"

    def __init__(self, queue_uri="http://localhost:5000@default"):
        super().__init__()

        r = re.search("(https?://.*?)@(.*)", queue_uri)
        if not r:
            raise Exception("uri does not match queue")

        self.leader = r.groups()[0]
        self.queue = r.groups()[1]

        self.logger = logging.getLogger(repr(self))

        self.logger.info("initialized")

    @property
    def client(self):
        if getattr(self, '_client', None) is None:
            http_client = RequestsClient()
    
            netloc = urlparse(self.leader).netloc
            logger.debug("using bearer token for %s : %s", netloc, self.token)

            http_client.set_api_key(
                             netloc, "Bearer "+ self.token,
                             param_name='Authorization', param_in='header'
                            )

            self._client = SwaggerClient.from_url(
                        self.leader.strip("/")+"/apispec_1.json",
                        config={'use_models': False},
                        http_client=http_client,
                    )
        return self._client

    @property
    def token(self) -> str:
        if self._token is None:
            for n, m in [
                    ("env DDA_TOKEN", lambda: os.environ.get('DDA_TOKEN').strip()), # type: ignore
                    ("~/.dda-token", lambda: open(os.environ.get('HOME')+"/.dda-token", "rt").read().strip()), # type: ignore
                    ("./.dda-token", lambda: open(".dda-token", "rt").read().strip()), # type: ignore
                    ]:
                try:
                    logger.debug("trying to get token with method %s", n)
                    self._token = m()
                    logger.info("managed to get token with method %s", n)
                    break
                except Exception as e:
                    logger.debug("failed to get token with method %s : %s", n, e)

            if self._token is None:
                logger.debug("all methods to get token failed using default empty")
                self._token = ""

        return self._token

