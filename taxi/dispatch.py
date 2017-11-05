from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import threading

import structlog

from taxi.util import callable_fqn as fqn


LOG = structlog.getLogger(__name__)


class Executor(object):
    """
    Utility for storing a list of functions and associated thread pool.

    Maintains a ThreadPoolExecutor and function registry.  Calling
    ``.dispatch()`` submits all registered functions in bulk with the
    arguments supplied to ``.dispatch()``
    """

    def __init__(self, max_workers, name=None):
        """Initiailize an Executor.

        :param int max_workers: ThreadPoolExecutor worker pool size
        :param string pattern: Optional channel pattern (used for logging)
        :param string name: Optional name (used for logging)
        """
        self.max_workers = max_workers
        self.name = name
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.registry = set()
        self.registry_lock = threading.RLock()
        self.log = LOG.bind(executor=name, max_workers=max_workers)

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
            log = LOG.bind(fqn=name, args=args, kwargs=kwargs)
            e = task.exception()
            if e:
                log.exception('Task raised an exception')
            else:
                try:
                    log.debug('Submitted task completed')
                except ValueError:
                    pass # Suppress during shutdown

        @wraps(func)
        def _wrapped_func(*args, **kwargs):
            log = LOG.bind(fqn=fqn(func))
            try:
                func(*args, **kwargs)
            except Exception as e:
                self.log.exception(exc_info=e)
                raise

        task = self.pool.submit(_wrapped_func, *args, **kwargs)
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

    def is_registered(self, callback):
        """Check if a callback is in the registry."""
        with self.registry_lock:
            return callback in self.registry

    def clear(self):
        """Clear all callbacks from the registry."""
        with self.registry_lock:
            self.registry.clear()
            self.log.debug('All callbacks removed')
            return True

    def dispatch(self, *args, **kwargs):
        """Dispatch all registered callbacks."""
        with self.registry_lock:
            tasks = [self.submit(cb, *args, **kwargs) for cb in self.registry]
            return tasks


class DispatchGroup(object):

    def __init__(self, name):
        self.log = LOG.bind(dispatch_group=name)
        self.name = name
        self.executors = dict()
        self._lock = threading.RLock()

    def __del__(self):
        self.clear_executors(wait=False)

    def init_executor(self, name, max_workers=None):
        """Instantiate a new executor and add it to the self.executors cache.

        :param string name: Unique executor name (e.g. sync/async)
        :param int max_workers: Executor thread pool size
        """
        with self._lock:
            log = self.log.bind(executor_name=name)
            if name in self.executors:
                log.warning('Executor already exists')
                return

            log.debug('Registering executor', max_workers=max_workers)
            ex = Executor(max_workers=max_workers,
                          name=name)
            self.executors[name] = ex
            return ex

    def remove_executor(self, name, wait=True):
        """Shutdown and remove a registered executor.

        :param string name: Unique executor name (e.g. sync/async)
        :param bool wait: Block until all tasks are complete
        """
        log = self.log.bind(executor_name=name,
                            wait=wait)
        with self._lock:
            try:
                ex = self.executors[name]
            except KeyError:
                log.error('Executor name does not exist')
                return False
            ex.shutdown(wait)
            log.info('Executor shutdown')

            del self.executors[name]
            return True

    def clear_executors(self, wait=True):
        names = list(self.executors.keys())
        self.log.debug('Shutting down executors', executors=names, wait=wait)
        return all([self.remove_executor(name, wait=wait) for name in names])

    def is_registered(self, callback, executor_name=None):
        """Check if a callback is already registered with an executor.

        :param string pattern: Subscription pattern
        :param func callback: Callback to check
        :param string name: If specified, look for specific name
        """
        log = self.log.bind(fqn=fqn(callback),
                            executor_name=executor_name)

        if executor_name is not None:
            try:
                executors = [self.executors[executor_name]]
            except KeyError:
                log.error('Executor does not exist')
                return False
        else:
            executors = self.executors.values()

        return any(ex.is_registered(callback) for ex in executors)

    def register(self, callback, executor_name):
        """Register a callback.

        :param func callback: Callback to register
        :param string executor_name: If specified, only execute
                                     callbacks under this executor name
        """
        log = self.log.bind(fqn=fqn(callback),
                            executor_name=executor_name)

        try:
            ex = self.executors[executor_name]
        except KeyError:
            log.error('Executor does not exist')
            return False
        return ex.register(callback)

    def unregister(self, callback, executor_name=None):
        """Remove a callback registered under a name.

        If name is omitted, attempt to unregister the callback from all names.

        :param string pattern: Exactly matching pattern string
        :param func callback: Callback to remove
        :param string executor_name: If specified, only remove callbacks with this name
        """
        log = self.log.bind(fqn=fqn(callback),
                            executor_name=executor_name)

        with self._lock:
            if executor_name is not None:
                try:
                    executors = [self.executors[executor_name]]
                except KeyError:
                    log.error('Executor does not exist')
                    return False
            elif not self.executors:
                log.error('No executors')
                return False
            else:
                executors = self.executors.values()

        results = [ex.unregister(callback) for ex in executors]
        return any(results)

    def unregister_all(self, executor_name=None, remove_executors=False, wait=True):
        """Remove all callbacks registered under a name.

        If name is omitted, attempt to unregister all callbacks
        registered under ``pattern``.

        :param string executor_name: If specified, only remove callbacks with this name
        :param bool remove_executors: Remove executors entirely
            instead of only clearing the registries
        :param bool wait: If remove_executors is True, wait for each
            executor to shutdown before returning
        """
        log = self.log.bind(executor_name=executor_name,
                            remove_executors=remove_executors,
                            wait=wait)
        with self._lock:
            if executor_name is not None:
                try:
                    executors = [self.executors[executor_name]]
                except KeyError:
                    log.error('Executor does not exist')
                    return False
            elif not self.executors:
                log.error('No executors')
                return False
            else:
                executors = self.executors.values()

            results = []
            for ex in executors:
                if remove_executors:
                    results += [self.remove_executor(ex.name, wait)]
                else:
                    results += [ex.clear()]
            log.debug('Unregistered all', results=results)
            return all(results)

    def dispatch(self, *args, **kwargs):
        """Dispatch callbacks registered under ``pattern`` by executor name.

        :param string pattern: Exactly matching pattern string
        :param string name: If specified, only execute callbacks with this name
        :param args: Arguments to pass to each callback
        :param kwargs: Keyword arguments to pass to each callback
        """
        log = self.log.bind(args=args, kwargs=kwargs)

        if not self.executors:
            log.error('No executors (e.g. no callbacks registered')
            return

        with self._lock:
            results = []
            for ex in self.executors.values():
                results += ex.dispatch(*args, **kwargs)
            return results


class Dispatcher(object):
    """
    Maintains DispatchGroups keyed by subscription pattern.

    """
    def __init__(self):
        self.log = LOG.bind()
        self.groups = dict()
        self._lock = threading.RLock()

    def get_group(self, group_name):
       log = self.log.bind(group_name=group_name)
       with self._lock:
           try:
               group = self.groups[group_name]
           except KeyError:
               log.error('Group does not exist')
               return
           return group

    def init_group(self, name, executors=None):
        """Instantiate a new group and add it to the cache.

        :param string name: Unique executor name (i.e. sync/async)
        :param iterable executors: Iterable of distinct args for init_executor
        """
        with self._lock:
            log = self.log.bind(group=name)
            if name in self.groups:
                log.warning('Group already exists')
                return

            log.debug('Registering group')
            group = DispatchGroup(name=name)
            self.groups[name] = group

            if executors:
                for args in executors:
                    log.debug('Initializing executors from config', args=args)
                    group.init_executor(*args)

            return group

    def remove_group(self, name, wait=True):
        """Shutdown and remove a group.

        :param string name: Unique executor name (e.g. sync/async)
        :param bool wait: Block until all tasks are complete
        """
        log = self.log.bind(group_name=name,
                            wait=wait)

        group = self.get_group(name)
        if not group:
            return False

        group.clear_executors(wait=wait)

        log.debug('Removing group')
        with self._lock:
            del self.groups[name]
        return True

    def is_registered(self, callback, group_name, executor_name=None):
        group = self.get_group(group_name)
        if not group:
            return False
        return group.is_registered(callback, executor_name)

    def register(self, callback, group_name, executor_name):
        group = self.get_group(group_name)
        if not group:
            return False
        return group.register(callback, executor_name)

    def unregister(self, callback, group_name, executor_name=None):
        group = self.get_group(group_name)
        if not group:
            return False
        return group.unregister(callback, executor_name)

    def unregister_all(self, group_name, executor_name=None, remove_executors=False, wait=True):
        group = self.get_group(group_name)
        if not group:
            return False
        return group.unregister_all(executor_name, remove_executors, wait)

    def dispatch(self, group_name, *args, **kwargs):
        with self._lock:
            try:
                group = self.groups[group_name]
            except KeyError:
                self.log.error('Group does not exist')
                return
        return group.dispatch(*args, **kwargs)
