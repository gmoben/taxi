from contextlib import contextmanager

import mock
import pytest

from taxi.dispatch import Executor, DispatchGroup, Dispatcher
from taxi.factory import ClientFactory, NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_concrete_engine, list_modules


ENGINE_MODULES = list_modules('taxi.engines')


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
def executor_name(expectations):
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
def alt_executor_name():
    return 'alt_executor_name'


@pytest.fixture()
def executor(request, executor_name, max_workers):
    e = Executor(max_workers=max_workers,
                 name=executor_name)
    if max_workers is not None:
        assert e.pool._max_workers == max_workers
    assert len(e.registry) == 0
    yield e
    e.shutdown(wait=False)


@pytest.fixture()
def dispatch_group(pattern):
    return DispatchGroup(pattern)


@pytest.fixture()
def loaded_group(dispatch_group, executor_name, max_workers, mock_functions):
    assert dispatch_group.init_executor(executor_name, max_workers)
    for f in mock_functions:
        assert dispatch_group.register(f, executor_name) is True
    yield dispatch_group
    dispatch_group.clear_executors()


@pytest.fixture()
def dispatcher():
    return Dispatcher()


@pytest.fixture()
def single_dispatcher(dispatcher, pattern, executor_name, max_workers, mock_functions):
    dispatcher.init_group(pattern, [(executor_name, max_workers)])
    for f in mock_functions:
        dispatcher.register(callback=f,
                            group_name=pattern,
                            executor_name=executor_name)
    yield dispatcher


@pytest.fixture()
def multi_dispatcher(single_dispatcher, executor_name, max_workers, alt_mock_functions, alt_pattern):
    # Register same functions under different patterns and executor_names
    disp = single_dispatcher
    disp.init_group(alt_pattern, [(executor_name, max_workers)])
    for f in alt_mock_functions:
        disp.register(callback=f,
                      group_name=alt_pattern,
                      executor_name=executor_name)
    yield disp


@pytest.fixture()
def mock_functions():
    return [mock.Mock(return_value=x) for x in range(10)]


@pytest.fixture()
def alt_mock_functions():
    return [mock.Mock(return_value=x) for x in range(10, 20)]


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
