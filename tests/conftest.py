import pytest

import dqueue.app

@pytest.fixture
def app():
    app = dqueue.app.app
    return app
