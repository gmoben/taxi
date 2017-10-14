import mock
import pytest

from taxi.util import get_concrete_engine


@pytest.fixture('function')
def nats():
    return get_concrete_engine('nats')


def test_matches_subject(nats):
    pass


def test_get_subtopic_pattern(nats):
    pass


def test_ping(nats):
    pass


def test_pong(nats):
    pass
