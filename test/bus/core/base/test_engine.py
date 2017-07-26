from concurrent.futures import ThreadPoolExecutor
import inspect
import time

import mock
import pytest


def test_connect(engine):
    engine.connect()
    assert engine.connected == True


def test_connected(engine):
    pass


def test_listen(engine):
    engine.connect()
    assert hasattr(engine.listen(), '__iter__')


def test_close(engine):
    engine.connect()
    assert engine.connected == True
    engine.close()
    assert engine.connected == False


def test_parse_message(engine):
    engine.parse_message('MSG 123')


def test_publish(engine):
    engine.publish('blah', '1234')


def test_subscribe(engine, engine_cls):
    channel = 'test'
    payload = 'payload'

    callback = mock.MagicMock(return_value=None)

    engine.connect()
    engine.subscribe(channel, callback)

    time.sleep(1)

    e2 = engine_cls()
    e2.connect()
    e2.publish(channel, payload)

    callback.assert_called_once()

def test_unsubscribe(engine, engine_cls):
    channel = 'test'
    payload = 'payload'

    with pytest.raises(Exception):
         client.unsubscribe(channel)

    callback = mock.MagicMock(return_value=None)

    engine.subscribe(channel, callback=callback)

    time.sleep(0.5)

    e2 = engine_cls()
    e2.connect()
    e2.publish(channel, payload)
    e2.close()

    callback.assert_called_once()

    engine.unsubscribe(channel)

    time.sleep(0.5)
    e2.publish(channel, payload)

    time.sleep(0.5)
    callback.assert_called_once()


def test_matches_subject(engine):
    assert engine.matches_subject('1234', '1234')
    assert not engine.matches_subject('1234', '1234.5678')
    assert engine.matches_subject('*', '1234')
    assert not engine.matches_subject('*.1234', '1234')
    assert engine.matches_subject('1234.*', '1234.5678')
    assert not engine.matches_subject('1234.*', '1234')
    # TODO: add more cases


def test_get_subtopic_pattern(engine):
    pass


def test_subscribe_subtopics(engine):
    pass
