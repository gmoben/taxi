from concurrent.futures import ThreadPoolExecutor
import random

import mock
import pytest

from taxi.core.callback import Executor
from taxi.constants import DEFAULT_MAX_WORKERS

LABEL_WORKERS = [
    ('sync', 1),
    ('async', DEFAULT_MAX_WORKERS),
    ('custom', 87),
    ('custom_no_max_workers', DEFAULT_MAX_WORKERS)
]

PATTERNS = ['dontmatter', 'shouldnt.care', 'q234qwafddasdf324Q@$Q#$']


@pytest.fixture(params=LABEL_WORKERS, ids=[x[0] for x in LABEL_WORKERS])
def executor(request):
    label, max_workers = request.param
    e = Executor(max_workers=max_workers,
                 pattern='foo.bar',
                 label=label)
    yield e
    e.shutdown(wait=True)


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


def validate_submissions(validations):
    """Helper method for validating execution of mocked submissions"""
    with ThreadPoolExecutor(max_workers=len(validations)) as e:

        def validate(args):
            submission, future = args
            func, args, kwargs = submission
            result = future.result(timeout=3)
            assert result == func.return_value
            func.assert_called_with(*args, **kwargs)
            return True

        results = e.map(validate, validations)
        assert all(results)


@pytest.mark.parametrize('pattern', PATTERNS)
@pytest.mark.parametrize('label,max_workers', LABEL_WORKERS)
def test_init(pattern, label, max_workers):
    e = Executor(max_workers=max_workers,
                 pattern=pattern,
                 label=label)
    assert e.pool._max_workers == max_workers
    assert len(e.registry) == 0


def test_submit(executor, mock_submissions):
    validations = []
    for submission in mock_submissions:
        func, args, kwargs = submission
        future = executor.submit(func, *args, **kwargs)
        validations.append((submission, future))
    validate_submissions(validations)


@pytest.mark.parametrize('wait', [True, False])
def test_shutdown(executor, wait):
    old_pool = executor.pool
    executor.shutdown(wait=wait)
    assert isinstance(executor.pool, ThreadPoolExecutor)
    assert executor.pool is not old_pool


def test_register(executor, mock_functions):
    assert len(executor.registry) == 0
    for f in mock_functions:
        result = executor.register(f)
        assert result is True
    assert executor.registry == set(mock_functions)


def test_unregister(executor, mock_functions):
    for f in mock_functions:
        executor.register(f)
    for _ in range(len(mock_functions)):
        func = random.choice(mock_functions)
        mock_functions.remove(func)
        assert executor.unregister(func) is True
        assert executor.registry == set(mock_functions)
    assert len(executor.registry) == 0


def test_clear_registry(executor, mock_functions):
    old_registry = executor.registry
    for f in mock_functions:
        executor.register(f)
    assert executor.registry == set(mock_functions)
    assert executor is not set(mock_functions)
    executor.clear_registry()
    assert len(executor.registry) == 0
    assert executor.registry is old_registry


def test_dispatch(executor, mock_functions, mock_args, mock_kwargs):
    for f in mock_functions:
        executor.register(f)
    tasks = executor.dispatch(*mock_args, **mock_kwargs)
    results = [task.result(timeout=3) for task in tasks]
    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
