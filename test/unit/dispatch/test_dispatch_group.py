from mock import Mock
import pytest


def test_init_executor(dispatch_group, executor_name, max_workers):
    group = dispatch_group
    executor = group.init_executor(executor_name, max_workers)
    assert len(group.executors) == 1
    assert executor_name in group.executors
    assert executor is group.executors[executor_name]
    if max_workers is not None:
        assert executor.pool._max_workers == max_workers
    else:
        assert executor.pool._max_workers is not None

    assert group.init_executor(executor_name, max_workers) is None
    assert group.init_executor(executor_name) is None


@pytest.mark.parametrize('wait', [True, False])
def test_remove_executor(loaded_group, executor_name, wait):
    group = loaded_group
    assert group.remove_executor('notaexecutor_name') is False
    assert executor_name in group.executors
    assert group.remove_executor(executor_name, wait) is True
    assert executor_name not in group.executors
    assert group.remove_executor(executor_name, wait) is False


@pytest.mark.parametrize('wait', [True, False])
def test_clear_executors(loaded_group, wait):
    group = loaded_group
    assert len(group.executors) == 1
    assert group.clear_executors(wait) is True
    assert len(group.executors) == 0


def test_is_registered(loaded_group, executor_name, mock_functions):
    group = loaded_group
    for f in mock_functions:
        assert group.is_registered(f) is True
        assert group.is_registered(f, executor_name) is True

def test_register(dispatch_group, mock_functions, executor_name, max_workers):
    group = dispatch_group
    assert not executor_name in group.executors
    assert group.register(Mock(), executor_name) is False
    group.init_executor(executor_name, max_workers)
    for f in mock_functions:
        assert group.register(callback=f, executor_name=executor_name) is True
    assert len(group.executors) == 1
    executor = group.executors[executor_name]
    assert executor.registry == set(mock_functions)


def test_unregister(dispatch_group, executor_name, alt_executor_name, max_workers, mock_functions):
    group = dispatch_group
    assert group.unregister(Mock()) is False
    assert group.unregister(Mock(), executor_name) is False
    assert group.init_executor(executor_name, max_workers)
    for f in mock_functions:
        assert group.register(f, executor_name) is True

    for f in mock_functions:
        assert group.is_registered(f, executor_name)
        assert group.unregister(f, executor_name) is True
        assert not group.is_registered(f, executor_name)
        assert group.unregister(f, executor_name) is False

    assert group.register(Mock(), alt_executor_name) is False
    assert group.init_executor(alt_executor_name, max_workers)
    for f in mock_functions:
        assert group.register(f, executor_name) is True
        assert group.unregister(f, alt_executor_name) is False
        assert group.register(f, alt_executor_name) is True
        assert group.unregister(f) is True
        assert group.is_registered(f, executor_name) is False
        assert group.is_registered(f, alt_executor_name) is False


@pytest.mark.parametrize('remove_executors', [True, False])
@pytest.mark.parametrize('wait', [True, False])
def test_unregister_all(dispatch_group, mock_functions, executor_name, max_workers, remove_executors, wait):
    group = dispatch_group
    assert group.unregister_all(executor_name) is False
    assert group.unregister_all(executor_name, remove_executors, wait) is False
    assert group.init_executor(executor_name, max_workers)
    for f in mock_functions:
        assert group.register(f, executor_name) is True
    assert group.unregister_all(executor_name, remove_executors, wait) is True
    for f in mock_functions:
        assert group.is_registered(f, executor_name) is False
        assert bool(executor_name in group.executors) is (False if remove_executors else True)
    if remove_executors is True:
        assert group.unregister_all(executor_name) is False
        assert group.unregister_all() is False
    else:
        for f in mock_functions:
            assert group.register(f, executor_name) is True
        assert group.unregister_all() is True


def test_dispatch(loaded_group, mock_functions, mock_args, mock_kwargs):
    group = loaded_group
    tasks = group.dispatch(*mock_args,
                           **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()

    group.clear_executors()
    assert group.dispatch(*mock_args, **mock_kwargs) is None
