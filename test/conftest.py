from contextlib import contextmanager

import mock
import pytest

from taxi.core.callback import Executor, Dispatcher
from taxi.core.factory import ClientFactory, NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_concrete_engine, list_modules


ENGINE_MODULES = list_modules('taxi.core.engines')


@pytest.fixture(
    params=[
        (True, 'sync', 1),
        (False, 'async', None),
        (None, 'custom', 87),
        (None, 'custom_no_max_workers', None)],
    ids=['sync', 'async', 'custom', 'noworkers'])
def expectations(request):
    return request.param


@pytest.fixture()
def sync(expectations):
    return expectations[0]


@pytest.fixture()
def label(expectations):
    return expectations[1]


@pytest.fixture()
def max_workers(expectations):
    return expectations[2]


@pytest.fixture(params=['test.pattern', '>', '*'])
def pattern(request):
    return request.param


@pytest.fixture()
def alt_pattern():
    return 'alt_pattern'


@pytest.fixture()
def alt_label():
    return 'alt_label'


@pytest.fixture()
def executor(request, label, max_workers):
    e = Executor(max_workers=max_workers,
                 pattern='foo.bar',
                 label=label)
    if max_workers is not None:
        assert e.pool._max_workers == max_workers
    assert len(e.registry) == 0
    yield e
    e.shutdown(wait=True)


@pytest.fixture()
def dispatcher():
    return Dispatcher()


@pytest.fixture()
def single_dispatcher(dispatcher, pattern, label, max_workers, mock_functions):
    for f in mock_functions:
        dispatcher.register(pattern=pattern,
                            callback=f,
                            label=label,
                            max_workers=max_workers)
    yield dispatcher


@pytest.fixture()
def multi_dispatcher(single_dispatcher, pattern, label, max_workers, mock_functions, alt_pattern, alt_label):
    # Register same functions under different patterns and labels
    disp = single_dispatcher
    for f in mock_functions:
        disp.register(pattern=alt_pattern,
                      callback=f,
                      label=label,
                      max_workers=max_workers)
        disp.register(pattern=pattern,
                      callback=f,
                      label=alt_label,
                      max_workers=max_workers)
    yield disp


@pytest.fixture()
def mock_functions():
    return [mock.Mock(return_value=x) for x in range(10)]


@pytest.fixture(
    params=[tuple(), (1,), (1, 2)],
    ids=['empty_args', 'single_arg', 'multi_args'])
def mock_args(request):
    return request.param


@pytest.fixture(
    params=[{}, {'a': 1}, {'a': 1, 'b': 2}],
    ids=['empty_kwargs', 'single_kwarg', 'multi_kwargs'])
def mock_kwargs(request):
    return request.param


@pytest.fixture()
def mock_submissions(request, mock_functions, mock_args, mock_kwargs):
    return [(func, mock_args, mock_kwargs) for func in mock_functions]


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
