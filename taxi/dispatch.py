from concurrent.futures import ThreadPoolExecutor
import threading

import structlog

from taxi.util import (
    callable_fqn as fqn,
    threadsafe_defaultdict as defaultdict
)

LOG = structlog.getLogger(__name__)


class Executor(object):
    """
    Utility for storing a list of functions and dispatching them
    asynchronously.

    Maintains a ThreadPoolExecutor and function registry.  Calling
    ``.dispatch()`` submits all registered functions in bulk with the
    arguments supplied to ``.dispatch()``
    """

    def __init__(self, max_workers, pattern=None, label=None):
        """Initiailize an Executor.

        :param string pattern: Optional channel pattern (used for logging)
        :param string label: Optional label (used for logging)
        :param int max_workers: ThreadPoolExecutor worker pool size
        """
        self.pattern = pattern
        self.label = label
        self.max_workers = max_workers
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.registry = set()
        self.registry_lock = threading.RLock()

        self.log = LOG.bind(pattern=pattern, label=label, max_workers=max_workers)

    def __del__(self):
        self.pool.shutdown(wait=False)

    def submit(self, func, *args, **kwargs):
        """Submit a task directly to the thread pool.

        :param func func: function to submit
        :param list args: function args
        :param dict kwargs: function kwargs
        """
        def on_complete(task, name, args, kwargs):
            """ Log useful info about task status """
            log = self.log.bind(fqn=name, args=args, kwargs=kwargs)
            e = task.exception()
            if e:
                log.exception(e)
            elif task.cancelled():
                log.info('Submitted task cancelled')
            else:
                log.debug('Submitted task completed')

        task = self.pool.submit(func, *args, **kwargs)
        task.add_done_callback(
            lambda task, n=fqn(func), a=args, k=kwargs: on_complete(task, n, a, k))
        self.log.debug('Submitted task', fqn=fqn(func), task=task)
        return task

    def shutdown(self, wait=True):
        """Shutdown the ThreadPoolExecutor.

        :param bool wait: Block until all tasks are complete
        """
        self.log.info('Executor shutting down', wait=wait)
        self.pool.shutdown(wait=wait)
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def register(self, callback):
        """Add a callback to the registry.

        :param func callback: Callback to register
        """
        with self.registry_lock:
            self.log.debug('Registering callback', callback=fqn(callback))
            self.registry.add(callback)
            return True

    def unregister(self, callback):
        """Remove a callback from registry.

        :param func callback: Callback to remove
        :returns: Success or failure
        :rtype: boolean
        """
        log = self.log.bind(fqn=fqn(callback))
        try:
            with self.registry_lock:
                self.registry.remove(callback)
            log.debug('Callback removed from registry')
            return True
        except KeyError:
            log.warning('Callback not registered')
            return False

    def clear_registry(self):
        """Clear all callbacks from the registry."""
        with self.registry_lock:
            self.registry.clear()
            self.log.debug('All callbacks removed')

    def dispatch(self, *args, **kwargs):
        """Dispatch all registered callbacks."""
        with self.registry_lock:
            tasks = [self.submit(cb, *args, **kwargs) for cb in self.registry]
            return tasks


class Dispatcher(object):
    """
    Maintains groups of Executors keyed by subscription pattern and
    label.

    Some functions have ``sync`` flags.
    If True:  label = 'sync' && max_workers = 1
    If False: label = 'sync' && max_workers = taxi.constants.DEFAULT_MAX_WORKERS
    If None: specify your own label and max_workers where appropriate
    """
    def __init__(self):
        self.log = LOG.bind()
        self.executors = defaultdict(dict)
        self._lock = threading.RLock()

    @property
    def patterns(self):
        with self._lock:
            return list(self.executors.keys())

    @property
    def labels(self):
        """Get a map of patterns to lists of executor labels."""
        with self._lock:
            return {k: list(self.executors[k].keys()) for k in self.executors}

    def init_executor(self, pattern, label, max_workers):
        """Instantiate a new executor and add it to the self.executors cache.

        :param string pattern: Pattern to match against incoming messages
        :param string label: Unique executor label (e.g. sync/async)
        :param int max_workers: Executor thread pool size
        """
        with self._lock:
            log = self.log.bind(pattern=pattern, label=label)
            if self.has_executor(pattern, label):
                log.warning('Executor already exists')
                return

            log.debug('Registering executor', max_workers=max_workers)
            ex = Executor(max_workers=max_workers,
                          pattern=pattern,
                          label=label)
            self.executors[pattern][label] = ex
            return ex

    def remove_executor(self, pattern, label, wait=True):
        """Shutdown and remove a registered executor.

        :param string pattern: Pattern to match against incoming messages
        :param string label: Unique executor label (e.g. sync/async)
        :param bool wait: Block until all tasks are complete
        """
        log = self.log.bind(pattern=pattern, label=label, wait=wait)
        with self._lock:
            if not self.has_executor(pattern, label):
                log.warning('Executor not registered')
                return False

            ex = self.executors[pattern][label]
            ex.shutdown(wait)
            log.info('Executor shutdown')

            del self.executors[pattern][label]
            if len(self.executors[pattern]) == 0:
                log.debug('Removing executor key')
                del self.executors[pattern]
            return True

    def has_executor(self, pattern, label=None):
        """Check if executors already exist.

        :param string pattern: Subscription pattern
        :param string label: If specified, look for specific label
        """
        with self._lock:
            if pattern in self.executors:
                ex = self.executors[pattern]
                return True if (label is None and len(ex) > 0) else label in ex
            return False

    def is_registered(self, pattern, callback, label=None):
        """Check if a callback is already registered with an executor.

        :param string pattern: Subscription pattern
        :param func callback: Callback to check
        :param string label: If specified, look for specific label
        """
        if self.has_executor(pattern, label):
            if label is None:
                return any(callback in ex.registry
                           for k, ex in self.executors[pattern].items())
            return callback in self.executors[pattern][label].registry
        return False

    def register(self, pattern, callback, label, max_workers=None):
        """Register a callback.

        :param string pattern: Exactly matching pattern string
        :param func callback: Callback to register
        :param string label: If specified, only execute callbacks with this label
        :param int max_workers: Worker count to use if executor doesn't already exist
        """
        log = self.log.bind(pattern=pattern, fqn=fqn(callback),
                            label=label, max_workers=max_workers)

        if label is None:
            log.error('Label cannot be None')
            return False

        if not self.has_executor(pattern, label):
            ex = self.init_executor(pattern, label, max_workers)
            log.debug('Created new executor', executor=ex)
        else:
            ex = self.executors[pattern][label]
            log.debug('Reusing existing executor', executor=ex)
            if max_workers != ex.max_workers:
                log.warning('Specified max_workers does not match existing executor',
                            executor=ex,
                            executor_max_workers=ex.max_workers)

        return ex.register(callback)

    def unregister(self, pattern, callback, label=None):
        """Remove a callback registered under a label.

        If label is omitted, attempt to unregister the callback from all labels.

        :param string pattern: Exactly matching pattern string
        :param func callback: Callback to remove
        :param bool sync: True sets label to 'sync', False to 'async', and None to `label`
        :param string label: If specified, only remove callbacks with this label
        """
        labels = self.labels[pattern] if label is None else [label]
        log = self.log.bind(pattern=pattern, fqn=fqn(callback), label=label)

        if not self.has_executor(pattern, label):
            log.warning('No executors found', executor_labels=self.labels)
            return False

        results = []
        for x in labels:
            ex = self.executors[pattern][x]
            results += [ex.unregister(callback)]
        return any(results)

    def unregister_all(self, pattern, label=None, remove_executors=False, wait=True):
        """Remove all callbacks registered under a label.

        If label is omitted, attempt to unregister all callbacks
        registered under ``pattern``.

        :param string pattern: Exactly matching pattern string
        :param func callback: Callback to remove
        :param string label: If specified, only remove callbacks with this label
        :param bool remove_executors: Remove executors entirely
            instead of only clearing the registries
        :param bool wait: If remove_executors is True, wait for each
            executor to shutdown before returning
        """
        log = self.log.bind(pattern=pattern, label=label, remove_executors=remove_executors, wait=wait)
        with self._lock:
            if not self.has_executor(pattern):
                log.warning('No executors found', executors=self.executors.keys())
                return False
            for x in self.labels[pattern]:
                if remove_executors:
                    self.remove_executor(pattern, x, wait)
                else:
                    self.executors[pattern][x].clear_registry()

    def dispatch(self, pattern, label, *args, **kwargs):
        """Dispatch callbacks registered under ``pattern`` by executor label.

        :param string pattern: Exactly matching pattern string
        :param string label: If specified, only execute callbacks with this label
        :param args: Arguments to pass to each callback
        :param kwargs: Keyword arguments to pass to each callback
        """
        log = self.log.bind(pattern=pattern, label=label, args=args, kwargs=kwargs)
        if not self.has_executor(pattern, label):
            log.error('Pattern has no registered callbacks')
            return
        with self._lock:
            tasks = []
            if label is None:
                executors = self.executors[pattern].values()
            else:
                executors = [self.executors[pattern][label]]
            for executor in executors:
                tasks += executor.dispatch(*args, **kwargs)
            return tasks

    def dispatch_all(self, pattern, *args, **kwargs):
        """Dispatch all callbacks registered under ``pattern``.

        :param string pattern: Exactly matching pattern string
        :param string label: If specified, only execute callbacks with this label
        :param args: Arguments to pass to each callback
        :param kwargs: Keyword arguments to pass to each callback
        """
        return self.dispatch(pattern, None, *args, **kwargs)
