import mock
import pytest


def test_get_group(dispatcher, pattern):
    group = dispatcher.init_group(pattern)
    assert dispatcher.get_group(pattern) == group


def test_init_group(dispatcher, pattern, alt_pattern, executor_name, max_workers):
    group = dispatcher.init_group(pattern, [(executor_name, max_workers)])
    assert dispatcher.init_group(pattern) is None
    assert len(dispatcher.groups) == 1
    assert dispatcher.get_group(pattern) == group
    assert pattern in dispatcher.groups
    executor = group.executors[executor_name]
    if max_workers is not None:
        assert executor.pool._max_workers == max_workers

    # No executors
    group2 = dispatcher.init_group(alt_pattern)
    assert len(group2.executors) == 0


@pytest.mark.parametrize('wait', [True, False])
def test_remove_group(single_dispatcher, pattern, wait):
    disp = single_dispatcher
    assert pattern in disp.groups
    assert disp.remove_group('notapattern') is False
    assert pattern in disp.groups
    assert disp.remove_group(pattern, wait) is True
    assert disp.remove_group(pattern) is False
    assert disp.remove_group(pattern, wait) is False


def test_is_registered(dispatcher, pattern, alt_pattern, executor_name, max_workers):
    func = mock.Mock()
    assert dispatcher.is_registered(func, pattern, executor_name) is False
    dispatcher.init_group(pattern, [(executor_name, max_workers)])
    dispatcher.register(func, pattern, executor_name)
    assert dispatcher.is_registered(func, pattern) is True
    assert dispatcher.is_registered(func, alt_pattern) is False


def test_register(dispatcher, pattern, alt_pattern, executor_name, max_workers, mock_functions):
    disp = dispatcher
    disp.init_group(pattern, [(executor_name, max_workers)])
    for f in mock_functions:
        assert disp.register(f, pattern, executor_name) is True
        assert disp.is_registered(f, pattern, executor_name) is True
        assert disp.register(f, alt_pattern, executor_name) is False


def test_unregister(single_dispatcher, pattern, alt_pattern, executor_name, mock_functions):
    disp = single_dispatcher
    for f in mock_functions:
        assert disp.is_registered(f, pattern, executor_name)

    for f in mock_functions:
        assert disp.unregister(f, alt_pattern, executor_name) is False
        assert disp.unregister(f, pattern, executor_name) is True
        assert not disp.is_registered(f, pattern, executor_name)


@pytest.mark.parametrize('remove_executors', [True, False])
@pytest.mark.parametrize('wait', [True, False])
def test_unregister_all(single_dispatcher, pattern, alt_pattern, executor_name, mock_functions, remove_executors, wait):
    disp = single_dispatcher
    for f in mock_functions:
        assert disp.is_registered(f, pattern, executor_name)

    assert disp.unregister_all(alt_pattern, executor_name, remove_executors, wait) is False
    assert disp.unregister_all(pattern, executor_name, remove_executors, wait) is True
    for f in mock_functions:
        assert not disp.is_registered(f, pattern, executor_name)


def test_dispatch(multi_dispatcher, pattern, alt_pattern,
                  mock_functions, alt_mock_functions, mock_args, mock_kwargs):
    disp = multi_dispatcher

    # Invalid attempts
    result = disp.dispatch('notagroup',
                           *mock_args,
                           **mock_kwargs)
    assert result is None

    # Standard call
    tasks = disp.dispatch(pattern,
                          *mock_args,
                          **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()

    for f in alt_mock_functions:
        f.assert_not_called()

    # Alternate pattern
    tasks = disp.dispatch(alt_pattern,
                          *mock_args,
                          **mock_kwargs)
    assert len(tasks) == len(alt_mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_not_called()

    for f in alt_mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()
