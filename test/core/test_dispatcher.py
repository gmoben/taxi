import pytest


def test_patterns(single_dispatcher, pattern, label, max_workers):
    disp = single_dispatcher
    assert disp.patterns == [pattern]
    new_pattern = 'newpattern'
    disp.init_executor(new_pattern, label, max_workers)
    assert sorted(disp.patterns) == sorted([pattern, 'newpattern'])
    disp.remove_executor(pattern, label)
    assert disp.patterns == [new_pattern]


def test_labels(single_dispatcher, pattern, label, max_workers):
    disp = single_dispatcher
    assert disp.labels == {pattern: [label]}
    new_pattern = 'newpattern'
    new_label = 'newlabel'
    disp.init_executor(new_pattern, new_label, max_workers)
    assert disp.labels == {pattern: [label], new_pattern: [new_label]}
    disp.remove_executor(pattern, label)
    assert disp.labels == {new_pattern: [new_label]}


def test_init_executor(dispatcher, pattern, label, max_workers):
    executor = dispatcher.init_executor(pattern, label, max_workers)
    assert len(dispatcher.executors) == 1
    assert len(dispatcher.executors[pattern]) == 1
    assert pattern in dispatcher.executors
    assert label in dispatcher.executors[pattern]
    assert executor is dispatcher.executors[pattern][label]


@pytest.mark.parametrize('wait', [True, False])
def test_remove_executor(single_dispatcher, pattern, label, wait):
    disp = single_dispatcher
    assert disp.remove_executor('notapattern', label) is False
    assert disp.remove_executor(pattern, 'notalabel') is False
    assert disp.has_executor(pattern, label) is True
    assert disp.remove_executor(pattern, label, wait) is True
    assert disp.has_executor(pattern, label) is False
    assert disp.remove_executor(pattern, label, wait) is False


def test_has_executor(dispatcher, pattern, label, max_workers):
    dispatcher.init_executor(pattern, label, max_workers)
    assert dispatcher.has_executor(pattern, label) is True

    assert dispatcher.has_executor(pattern, 'notalabel') is False
    assert dispatcher.has_executor('notapattern', label) is False

    assert dispatcher.remove_executor(pattern, label) is True
    assert dispatcher.has_executor(pattern, label) is False


def test_is_registered(single_dispatcher, pattern, sync, label, mock_functions):
    disp = single_dispatcher
    for f in mock_functions:
        assert disp.is_registered(pattern, f)
        disp.unregister(pattern, f, sync, label)
        assert not disp.is_registered(pattern, f)


def test_register(dispatcher, pattern, mock_functions, sync, label, max_workers):
    label = 'sync' if sync else 'async'
    max_workers = 1 if sync else None
    assert not dispatcher.has_executor(pattern, label)
    for f in mock_functions:
        dispatcher.register(pattern=pattern,
                            callback=f,
                            sync=sync,
                            label=label)
    assert dispatcher.has_executor(pattern, label)
    assert len(dispatcher.executors) == 1
    executor = dispatcher.executors[pattern][label]
    assert executor.registry == set(mock_functions)
    if max_workers is not None:
        assert executor.pool._max_workers == max_workers
    else:
        assert executor.pool._max_workers is not None


def test_unregister(single_dispatcher, pattern, label, mock_functions, sync):
    disp = single_dispatcher
    for f in mock_functions:
        assert disp.is_registered(pattern, f, label) is True
        assert disp.unregister(pattern, f, sync, label) is True
        assert disp.is_registered(pattern, f, label) is False


@pytest.mark.parametrize('remove_executors', [True, False])
@pytest.mark.parametrize('wait', [True, False])
def test_unregister_all(single_dispatcher, mock_functions, pattern, label, sync, remove_executors, wait):
    disp = single_dispatcher
    for f in mock_functions:
        assert disp.is_registered(pattern, f, label) is True
    disp.unregister_all(pattern=pattern,
                        sync=sync,
                        label=label,
                        remove_executors=remove_executors,
                        wait=wait)
    for f in mock_functions:
        assert disp.is_registered(pattern, f, label) is False
        assert disp.has_executor(pattern, label) is (False if remove_executors else True)


def test_dispatch(multi_dispatcher, pattern, sync, label, alt_pattern, alt_label,
                  mock_functions, mock_args, mock_kwargs):
    disp = multi_dispatcher

    # Invalid attempts
    result = disp.dispatch('notapattern',
                           label,
                           *mock_args,
                           **mock_kwargs)
    assert result is None

    result = disp.dispatch(pattern,
                           'notalabel',
                           *mock_args,
                           **mock_kwargs)
    assert result is None

    # Standard call
    tasks = disp.dispatch(pattern,
                          label,
                          *mock_args,
                          **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()

    # Alternate pattern
    tasks = disp.dispatch(alt_pattern,
                          label,
                          *mock_args,
                          **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()

    # Alternate label
    tasks = disp.dispatch(pattern,
                          alt_label,
                          *mock_args,
                          **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()


def test_dispatch_all(multi_dispatcher, pattern, alt_pattern,
                      mock_functions, mock_args, mock_kwargs):
    disp = multi_dispatcher
    tasks = disp.dispatch_all(pattern,
                              *mock_args,
                              **mock_kwargs)
    assert len(tasks) == (len(mock_functions) * 2)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        assert f.call_count == 2
        first_args = f.call_args_list[0]
        assert all([x == first_args for x in f.call_args_list])
        assert len([x for x in results if x == f.return_value]) == 2
        f.reset_mock()

    tasks = disp.dispatch_all(alt_pattern,
                              *mock_args,
                              **mock_kwargs)
    assert len(tasks) == len(mock_functions)
    results = [t.result(timeout=3) for t in tasks]

    for f in mock_functions:
        f.assert_called_once_with(*mock_args, **mock_kwargs)
        assert f.return_value in results
        f.reset_mock()
