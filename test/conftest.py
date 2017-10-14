from contextlib import contextmanager

import pytest

from taxi.core.base import ClientFactory, NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_concrete_engine, list_modules, subtopic


ENGINE_MODULES = list_modules('taxi.core.engines')


@contextmanager
def _engine(name, *args, **kwargs):
    yield get_concrete_engine(name)


@pytest.fixture(scope='function',
                params=ENGINE_MODULES)
def engine_cls(request):
    with _engine(request.param) as cls:
        yield cls


@pytest.fixture(scope='function')
def engine(engine_cls):
    e = engine_cls()
    e.connect()
    yield e
    e.disconnect()


@pytest.fixture(scope='function')
def client(engine_cls):
    yield ClientFactory(engine_cls)


@pytest.fixture(scope='function')
def node(engine_cls):
    yield NodeFactory(engine_cls, 'test')


@pytest.fixture(scope='function')
def manager(engine_cls):
    yield ManagerFactory(engine_cls, 'test')


@pytest.fixture(scope='function')
def worker(engine_cls):
    yield WorkerFactory(engine_cls, 'test')
