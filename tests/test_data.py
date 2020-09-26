import pytest
import json
from flask import url_for
import os
import base64

import logging
logging.basicConfig(level=logging.DEBUG)

import requests

from dqueue import from_uri

@pytest.mark.usefixtures('live_server')
class TestLiveServer:
    
    @property
    def queue(self):
        if not hasattr(self, '_queue'):
            u = url_for("healthcheck", _external=True)
            print("u:", u)
            self._queue = from_uri(u+"@default")

        return self._queue

    def test_assert_large(self):
        u = url_for("data_assert_fact", _external=True)

        # schema here
        dag = [ "F", ["a1", "a2", [ "G", "b1", "b2"] ] ]
        data = base64.b64encode(os.urandom(1024)).decode()


        r = self.queue.assert_fact(dag, data)

        print(r)

        payload = self.queue.consult_fact(dag)

        redag = json.loads(payload['dag_json'])
        redata = json.loads(payload['data_json'])

        assert data == redata
        assert dag == redag
        
        light_payload = self.queue.consult_fact(dag, return_data=False)

        assert light_payload

        print("light payload:", light_payload)
