from concurrent.futures import ThreadPoolExecutor
import inspect
import time

import mock
import pytest


def test_connect(engine_cls):
    e = engine_cls()
    e.connect()
    assert e.connected == True


def test_listen(engine):
    assert hasattr(engine.listen(), '__iter__')
    msg = next(engine.listen())
    assert msg


def test_disconnect(engine):
    engine.connect()
    assert engine.connected is True
    engine.disconnect()
    assert engine.connected is False


def test_parse_message(engine):
    engine.parse_message('MSG\r\n123')


def test_publish(engine):
    engine.publish('blah', '1234')


def test_subscribe(engine, engine_cls):
    channel = 'test'
    data = 'data'

    callback = mock.MagicMock(return_value=None)

    msgs = engine.listen()
    assert next(msgs).startswith('INFO')

    engine.subscribe(channel, callback)
    assert next(msgs).startswith('+OK')

    e2 = engine_cls()
    e2.connect()
    e2.publish(channel, data)

    assert next(msgs).startswith('MSG')


def wait_for_callback(callback):
    for _ in range(5):
        try:
            callback.assert_called_once()
            print("Recieved")
            return True
        except:
            print("Not recieved")
            time.sleep(1)
    return False


def test_unsubscribe(engine, engine_cls):
    channel = 'test'
    data = 'data'

    assert engine.unsubscribe(channel) is False

    callback = mock.MagicMock(return_value=None)

    assert engine.subscribe(channel, callback=callback) is True

    e2 = engine_cls()
    e2.connect()
    e2.publish(channel, data)
    e2.disconnect()

    engine.unsubscribe(channel)

    e2.publish(channel, data)


def test_pattern_match(engine):
    assert engine.pattern_match('1234', '1234')
    assert not engine.pattern_match('1234', '1234.5678')
    assert engine.pattern_match('*', '1234')
    assert not engine.pattern_match('*.1234', '1234')
    assert engine.pattern_match('1234.*', '1234.5678')
    assert not engine.pattern_match('1234.*', '1234')
    # TODO: add more cases


def test_get_subtopic_pattern(engine):
    pytest.skip()


def test_subscribe_subtopics(engine):
    pytest.skip()
