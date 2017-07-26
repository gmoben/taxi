import inspect

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


def test_subscribe(engine_cls):
    engine1 = engine_cls()
    engine2 = engine_cls()

    engine1.connect()
    engine2.connect()

    callback = mock.MagicMock()

    engine1.subscribe('blah', callback)

    engine2.publish('blah', 'payload')

    msg = {
        'subject': 'blah',
        'payload': 'payload'
    }

    assert callback.assert_called_once_with(msg)


def test_unsubscribe(engine_cls):
    engine1 = engine_cls()
    engine2 = engine_cls()

    engine1.connect()
    engine2.connect()

    # with pytest.raises(Exception):
    #     engine1.unsubscribe('1234')

    callback = mock.MagicMock()

    msg = {
        'subject': '1234',
        'payload': 'payload'
    }

    engine1.subscribe('1234', callback)

    engine2.publish('1234', 'payload')

    callback.assert_called_once_with(msg)

    engine1.unsubscribe('1234')

    engine2.publish('1234', 'payload')

    callback.assert_called_once_with(msg)


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
