import mock
import pytest

from taxi.util import get_concrete_engine


@pytest.fixture('function')
def nats():
    return get_concrete_engine('nats')


def test_pattern_match(nats):
    pytest.skip()


def test_get_subtopic_pattern(nats):
    pytest.skip()


def test_ping(nats):
    pytest.skip()


def test_pong(nats):
    pytest.skip()
