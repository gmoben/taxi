import threading
import time
import uuid
from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import six
from six.moves.queue import Queue
import structlog

from taxi.constants import DEFAULT_MAX_WORKERS
from taxi.subjects import HA
from taxi.util import (
    Wrappable,
    callable_fqn as fqn,
    memoize,
    subtopic,
    threadsafe_defaultdict as defaultdict
)


LOG = structlog.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class AbstractEngine(object):
    """Implement this and pass the subclass into a Factory method"""

    def __init__(self, host=None, port=None, attempt_reconnect=True, *args, **kwargs):
        """Engine initialization.

        :param string host: Server hostname/IP
        :param string port: Server port
        :param boolean attempt_reconnect: Attempt reconnection to server if lost

        """
        self.host = host
        self.port = port
        self.attempt_reconnect = attempt_reconnect
        super(AbstractEngine, self).__init__(*args, **kwargs)

    @abstractmethod
    def connect(self, host=None, port=None):
        """Connect to the server instance

        :param string host: Server hostname/IP
        :param string port: Server port

        """
        raise NotImplementedError

    @abstractproperty
    def connected(self):
        """Inspect if the client is currently connected

        :returns: connection status
        :rtype: boolean

        """
        raise NotImplementedError

    @abstractmethod
    def listen(self):
        """Retrieve messages from the server

        :returns: collection of messages
        :rtype: iterable

        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        """Close the active server connection

        :returns: Success status
        :rtype: boolean

        """
        raise NotImplementedError

    @abstractmethod
    def parse_message(self, msg):
        """Transform incoming messages before executing callbacks.

        :param string msg: The raw incoming message
        :returns: Parsed message containing the keys 'subject', 'payload', and 'meta'

        """
        # Build an empty message with the raw message contents as the payload
        parsed_message = defaultdict(str)
        parsed_message['payload'] = msg
        return parsed_message

    @abstractmethod
    def publish(self, subject, payload, wait=False, **options):
        """Publish a payload to a subject.

        :param string subject: The destination channel
        :param string payload: UTF-8 encoded message payload
        :param boolean wait: Wait for reciept from server
        :param **options: Any additional options for the implementation

        :returns: Success status
        :rtype: boolean

        """
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, subject, callback, wait=False, **options):
        """Subscribe to a subject literally.

        :param string subject: The subject name to subscribe to
        :param function callback: Callback to execute when a message
            is received from this subject
        :param boolean wait: Wait for reciept from server
        :param **options: Any additional options for the implementation

        :return: Unique identifier of the resulting subscription
        :rtype: string

        """
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self, subject):
        """Unsubscribe by subscription id.

        :param string subject: The subject pattern to unsubscribe from

        """
        raise NotImplementedError

    def fuzzy_match(self, pattern, subject):
        """Determine if a given subject pattern matches a string.

        Defaults to performing an equality comparison between the parameters.

        :param string pattern: Pattern to match
        :param string string: String to match
        :returns: Whether or not the pattern matches the string
        :rtype: boolean

        """
        return pattern == subject

    def get_subtopic_pattern(self, subject, shallow):
        """Build a pattern for matching subtopics of a subject.

        :param subject: The base subject
        :param boolean shallow: If set, build subject matching direct children.
            Otherwise build a subject matching all descendents of a subject.
        :returns: The subtopic subject
        :rtype: string

        """
        return '.'.join([subject, '*'])

    def subscribe_subtopics(self, subject, callback, shallow=True):
        """Subscribe to subtopics of a subject.

        :param string subject: root subject
        :param function callback: Callback to execute when receiving a message
            from any subtopic
        :param boolean shallow: If set, only subscribe to direct children.
            Otherwise subscribe to all descendents of a subject.

        """
        subject = self.get_subtopic_pattern(subject, shallow)
        self.subscribe(subject, callback)


class Executor(object):
    """
    Utility for storing a list of functions and dispatching them
    asynchronously.

    Maintains a ThreadPoolExecutor and function registry.  Calling
    ``.dispatch()`` submits all registered functions in bulk with the
    arguments supplied to ``.dispatch()``
    """

    def __init__(self, pattern=None, label=None, max_workers=1):
        """Initiailize an Executor.

        :param string pattern: Optional channel pattern (used for logging)
        :param string label: Optional label (used for logging)
        :param int max_workers: ThreadPoolExecutor worker pool size
        """
        self.pattern = pattern
        self.label = label
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.registry = set()
        self.registry_lock = threading.RLock()

        self.log = LOG.bind(pattern=pattern, label=label, max_workers=max_workers)

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

        task = self.executor.submit(func, *args, **kwargs)
        task.add_done_callback(
            lambda task, n=fqn(func), a=args, k=kwargs: on_complete(task, n, a, k))
        self.log.debug('Submitted task', fqn=fqn(func), task=task)
        return task

    def shutdown(self, wait=True):
        """Shutdown the ThreadPoolExecutor.

        :param bool wait: Block until all tasks are complete
        """
        self.log.info('Executor shutting down', wait=wait)
        self.executor.shutdown(wait=wait)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def dispatch(self, *args, **kwargs):
        """Dispatch all registered callbacks."""
        with self.registry_lock:
            for cb in self.registry:
                self.submit(cb, *args, **kwargs)

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
            self.registry = set()
            self.log.debug('All callbacks removed')


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
            self.executors[pattern][label] = Executor(pattern, label, max_workers)

    def get_executor(self, pattern, label):
        if self.has_executor(pattern, label):
            return self.executors[pattern][label]

    def remove_executor(self, pattern, label, wait=True):
        """Shutdown and remove a registered executor.

        :param string pattern: Pattern to match against incoming messages
        :param string label: Unique executor label (e.g. sync/async)
        :param bool wait: Block until all tasks are complete
        """
        log = self.log.bind(pattern=pattern, label=label, wait=wait)
        with self._lock:
            if self.has_executor(pattern, label):
                ex = self.get_executor(pattern, label)
                ex.shutdown(wait)
                log.info('Executor shutdown')

                del self.executors[pattern][label]
                if len(self.executors[pattern]) == 0:
                    log.debug('Removing executor key')
                    del self.executors[pattern]
                else:
                    LOG.warning('Executor not registered')

    def register(self, pattern, callback, sync=False, label=None, max_workers=None):
        """Register a callback.

        :param string pattern: Exactly matching pattern string
        :param string label: If specified, only execute callbacks with this label
        """
        label = label or ('sync' if sync is True else ('async' if sync is False else label))
        max_workers = 1 if sync else (DEFAULT_MAX_WORKERS if max_workers is None else max_workers)
        log = self.log.bind(pattern=pattern, fqn=fqn(callback),
                            sync=sync, label=label, max_workers=max_workers)

        if label is None:
            log.error('Missing sync or label argument')
            return False

        if not self.has_executor(pattern, label):
            self.init_executor(pattern, label, max_workers)

        ex = self.get_executor(pattern, label)
        ex.register(callback)

    def unregister(self, pattern, callback, sync=None, label=None):
        """Remove a callback.

        :param string pattern: Exactly matching pattern string
        :param func callback: Callback to remove
        :param bool sync: True sets label to 'sync', False to 'async', and None to `label`
        :param string label: If specified, only remove callbacks with this label
        """
        label = 'sync' if sync is True else ('async' if sync is False else label)
        labels = self.labels[pattern] if label is None else [label]
        log = self.log.bind(pattern=pattern, fqn=fqn(callback), sync=sync, label=label)

        if not self.has_executor(pattern, label):
            log.warning('No executors found', executors=self.executors.keys())
            return False

        for x in labels:
            ex = self.get_executor(pattern, x)
            ex.unregister(callback)

    def unregister_all(self, pattern, sync=None, label=None, remove_executors=False, wait=True):
        log = self.log.bind(pattern=pattern, sync=sync, label=label)
        with self._lock:
            if not self.has_executor(pattern):
                log.warning('No executors found', executors=self.executors.keys())
                return False
            for x in self.labels[pattern]:
                if remove_executors:
                    self.remove_executor(pattern, x, wait)
                else:
                    self.executors[pattern][x].clear_registry()

    def dispatch(self, pattern, *args, label=None, **kwargs):
        """Dispatch all callbacks registered under `pattern`.

        :param string pattern: Exactly matching pattern string
        :param string label: If specified, only execute callbacks with this label
        """
        log = self.log.bind(pattern=pattern, label=label, args=args, kwargs=kwargs)
        if not self.has_executor(pattern, label):
            log.error('Pattern has no registered callbacks')
            return
        with self._lock:
            for ex_label, executor in self.executors[pattern].items():
                if label is None or label == ex_label:
                    executor.dispatch(*args, **kwargs)



@six.add_metaclass(ABCMeta)
class AbstractClient(Wrappable):
    """Mixin for AbstractEngine with callback management and convenience methods"""

    def __init__(self, *args, **kwargs):
        super(AbstractClient, self).__init__(self, *args, **kwargs)
        self.guid = str(uuid.uuid4())
        self.log = LOG.bind(guid=self.guid)

        self.dispatcher = Dispatcher()
        self.subscription_queue = []

        self.after('connect', self.flush_callbacks)
        self.override('listen', self.consume)
        self.after('parse_message', self.handle_message)
        self.before('subscribe', self.register_callback)
        self.override('subscribe', self.queue_subscribe)
        self.after('unsubscribe', self.unregister_callbacks)

    def flush_subscriptions(self, *args, **kwargs):
        """Subscribe to any subjects with registered callbacks.

        Executes immediately after ``Engine.connect``
        """
        log = self.log.bind(args=args, kwargs=kwargs)
        log.debug('Flushing queued subscriptions',
                  queue_depth=len(self.subscription_queue),
                  queue=self.subscription_queue)

        for q_args, q_kwargs in list(self.subscription_queue):
            self.subscribe(*q_args, **q_kwargs)
        self.subscription_queue = []

    def consume(self, imethod, *args, **kwargs):
        """Run a blocking consumer thread"""
        log = self.log.bind(imethod=imethod, args=args, kwargs=kwargs)
        try:
            # Use a ThreadPoolExecutor to help catch exceptions
            with ThreadPoolExecutor(max_workers=1) as ex:
                ex.map(self.parse_message, imethod())
        except:
            log.exception('Unhandled error while parsing messages')

    def handle_message(self, msg, *args, **kwargs):
        """Dispatch callbacks for matching dispatch patterns.

        :param dict msg: Parsed message returned from ``Engine.parse_message``
        """
        subject = msg['subject']
        log = self.log.bind(msg=msg, args=args, kwargs=kwargs)
        for pattern in self.dispatcher.patterns:
            if self.fuzzy_match(pattern, subject):
                log.debug('Matching pattern found', pattern=pattern)
                self.dispatcher.dispatch(pattern, msg, *args, **kwargs)

    def register_callback(self, pattern, callback, *args, **kwargs):
        self.dispatcher.register(pattern, callback)

    def unregister_callback(self, _, subject, *args, **kwargs):
        self.callback_manager.unregister_all(subject, remove_executors=True, wait=False)

    def queue_subscription(self, imethod, *args, **kwargs):
        """Queue a subscription if not currently connected"""
        if self.connected:
            imethod(*args, **kwargs)
        else:
            self.subscription_queue.append((args, kwargs))

    def request(self, subject, payload, callback, timeout):

        reply_to = subtopic('INBOX', self.guid)

        self.subscribe(reply_to, callback)
        self.publish(subject, payload, reply_to=reply_to)

        time.sleep(timeout)
        self.unsubscribe(reply_to)


class AbstractNode(AbstractClient):
    """Convenience class that executes ``self.setup`` then automatically connects"""
    NAMESPACE = None

    def __init__(self, *args, **kwargs):
        super(AbstractNode, self).__init__(self, *args, **kwargs)

        LOG.info('Starting %s [%s]', self.__class__.__name__, self.NAMESPACE)

        self.setup()
        self.connect()

    def setup(self):
        """Setup the instance prior to connecting (subscribing and setting up callbacks, etc.)"""
        pass


class AbstractManager(AbstractNode):

    def poll_workers(self, timeout=5):
        """Send a status request to workers in this namespace.

        :param timeout: Length of time to wait for responses
        :returns: Parsed response messages
        :rtype: list

        """

        responses = []

        def on_response(msg):
            responses += msg

        self.request(subtopic(HA.STATUS, self.NAMESPACE), None, on_response, timeout)

        return responses

    def publish_work(self, payload, worker_id=None):
        """Publish a work payload.

        If ``worker_id`` is defined, send payload to a specific worker without broadcasting.

        :param string payload: Payload to deliever
        :param string worker_id: Specific worker ID to recieve payload (don't broadcast)

        """
        if worker_id:
            self.publish(subtopic(HA.WORK, self.NAMESPACE, worker_id), payload)
        else:
            self.publish(subtopic(HA.WORK, self.NAMESPACE), payload)


class AbstractWorker(AbstractNode):
    """Worker node acting on work in its NAMESPACE"""

    def __init__(self, *args, **kwargs):
        """Initialize the worker and subscribe to work subjects for the namespace"""
        super(AbstractWorker, self).__init__(*args, **kwargs)

        self.after('connect', self._subscribe_ha)

    def _subscribe_ha(self, *args, **kwargs):
        self.subscribe(subtopic(HA.WORK, self.NAMESPACE), self.run, queue_group=self.NAMESPACE)
        self.subscribe(subtopic(HA.WORK, self.NAMESPACE, str(self.guid)), self.run)
        self.subscribe(subtopic(HA.STATUS, self.NAMESPACE), lambda msg: self.publish(msg['reply_to'], self.status))

    def status(self):
        return '{} LISTENING'.format(self.guid)

    def on_msg(self, msg):
        """Define this in your subclass"""
        pass


@memoize
def ClientFactory(engine_class):
    """Build a Client using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteClient class

    """
    class ConcreteClient(engine_class, AbstractClient):
        pass

    return ConcreteClient


@memoize
def NodeFactory(engine_class, *namespaces):
    """Build a Node using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteNode class

    """
    class ConcreteNode(engine_class, AbstractNode):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteNode


@memoize
def ManagerFactory(engine_class, *namespaces):
    """Build a Manager using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteManager class

    """
    class ConcreteManager(engine_class, AbstractManager):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteManager


@memoize
def WorkerFactory(engine_class, *namespaces):
    """Build a Worker using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteWorker class

    """
    class ConcreteWorker(engine_class, AbstractWorker):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteWorker
