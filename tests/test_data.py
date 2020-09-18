import pytest
import json
from flask import url_for

import logging
logging.basicConfig(level=logging.DEBUG)

import requests

@pytest.mark.usefixtures('live_server')
class TestLiveServer:

    def test_assert(self):
        u = url_for("data_assert_fact", _external=True)

        requests.post(u)

