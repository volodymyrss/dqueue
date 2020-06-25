import pytest

import dqueue.api

@pytest.fixture(scope="session")
def app():
    app = dqueue.api.app
    return app
