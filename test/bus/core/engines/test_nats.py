import mock
import pytest

from taxi.util import get_concrete_engine, server_context


@pytest.fixture('function')
def nats():
    with server_context('nats') as context:
        _, cls = context
        yield cls(host='0.0.0.0')


def test_matches_subject(nats):
    pass


def test_get_subtopic_pattern(nats):
    pass


def test_ping(nats):
    pass


def test_pong(nats):
    pass
