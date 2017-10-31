from concurrent.futures import ThreadPoolExecutor
import random

import pytest


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
    _ = [task.result(timeout=3) for task in tasks]
    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
