import concurrent.futures
import logging
import sys
import threading
import time
import types
import uuid
from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import six
from six.moves.queue import Queue

from bus.subjects import HA
from bus.util import Wrappable, callable_fqn, threadsafe_defaultdict as defaultdict, memoize, subtopic


logging.basicConfig(stream=sys.stdout)
LOG = logging.getLogger(__name__)


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
    def close(self):
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

    def matches_subject(self, pattern, subject):
        """Determine if a given subject pattern matches a string.

        Defaults to performing an equality comparison between the parameters.

        :param string pattern: Pattern to match
        :param string string: String to match
        :returns: Whether or not the pattern matches the string
        :rtype: boolean

        """
        return subject == pattern

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


class CallbackManager(object):

    def __init__(self, client):
        """Initiailize a CallbackManager.

        :param AbstractClient client: Parent client
        """
        self.client = client

        self._cache_lock = threading.RLock()
        self._callback_registry = defaultdict(lambda: defaultdict(list))
        self._queues = defaultdict(lambda: defaultdict(dict))
        self._dispatchers = defaultdict(dict)

        self.async_worker_count = 40 # TODO: Decide how to set this dynamically

    @contextmanager
    def locked_cache(self, subject):
        with self._cache_lock:
            if not subject in self._queues:
                self.start_dispatchers(subject)
            yield self._callback_registry[subject], self._queues[subject]

    def start_dispatcher(self, subject, tag, max_workers):
        with self._cache_lock:
            if tag in self._queues[subject]:
                raise RuntimeError('Queue already exists for %s[%s]', subject, tag)

            queue = Queue()

            def handle():
                while True:
                    cb, msg = queue.get()
                    cb(msg)

            def on_complete(task):
                exeception = task.exception()
                if e:
                    LOG.error(e)
                elif task.cancelled():
                    LOG.info('%s dispatcher cancelled', tag)
                else:
                    LOG.info('%s dispatcher stopped', tag)
                del self._queues[subject]
                del self._dispatchers[subject][tag]

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                task = ex.submit(handle)
                task.add_done_callback(on_complete)

            self._queues[subject][tag] = queue
            self._dispatchers[subject][tag] = task

    def start_dispatchers(self, subject):
        try:
            start_dispatcher(subject, 'sync', 1)
            start_dispatcher(subject, 'async', self.async_worker_count)
        except:
            LOG.exception('Error starting dispatchers for %s', subject)

    def dispatch_callbacks(self, msg):
        """Dispatch callbacks for matching subject keys.

        :param dict msg: Parsed message

        """
        subject = msg['subject']

        with self._cache_lock:
            for pattern in self._callback_registry:
                if self.client.matches_subject(pattern, subject):
                    with self.locked_cache(pattern) as (callbacks, queues):
                        for tag in ['sync', 'async']:
                            for cb in callbacks[tag]:
                                queues[tag].put_nowait(cb, msg)

    def add_callback(self, pattern, callback, sync):
        """Add a callback indexed by a subject pattern.

        :param string pattern: key of callback index
        :param func callback: Callback to execute
        :param boolean sync: If set, enable callback queuing

        """
        if callback:
            LOG.debug('Adding callback %s to %s', callable_fqn(callback), pattern)
            with self.locked_cache(pattern) as callbacks, _:
                if sync:
                    callbacks['sync'].append(callback)
                else:
                    callback['async'].append(callback)
        else:
            LOG.debug('No callback passed for pattern %s', pattern)

    def remove_callback(self, pattern, callback, tag=None, literal=False):
        """Remove a callbacks matching the pattern.

        :param string pattern: Pattern to search against for matching callbacks
        :param func callback: Callback to remove
        :param tag: type of callback (e.g. sync/async)
        :param boolean literal: If set, skip matching
        :returns: Removal success or failure
        :rtype: boolean

        """
        success = False
        with self._cache_lock:
            if literal:
                try:
                    self._callback_registry[pattern][tag].remove(callback)
                    success = True
                except ValueError:
                    pass
            else:
                for subject in self._callback_registry:
                    if self.client.matches_subject(subject, pattern):
                        callbacks = self._callback_registry[subject]
                        if tag and tag in callbacks:
                            try:
                                callbacks[tag].remove(callback)
                                success = True
                            except ValueError:
                                pass
                        elif not tag or not callbacks:
                            del self._callback_registry[subject]
                            success = True

        if not success:
            LOG.error('No callback %s found matching pattern %s [literal=%s]',
                      callable_fqn(callback), pattern, literal)
        return success

    def remove_callbacks(self, pattern, literal=False):
        """Remove all callbacks matching a subject pattern.

        :param string pattern: The subject to remove callbacks from

        """
        if literal:
            matches = lambda x, y: x == y
        else:
            matches = self.client.matches_subject

        with self._cache_lock:
            for key in self._callback_registry:
                if matches(pattern, key):
                    callback_names = map(callable_fqn, self._callback_registry[key])
                    LOG.debug('Removing all callbacks for %s: %s', key, callback_names)
                    del self._callback_registry[key]


@six.add_metaclass(ABCMeta)
class AbstractClient(Wrappable):
    """Mixin for AbstractEngine with callback management and convenience methods"""

    def __init__(self, *args, **kwargs):
        super(AbstractClient, self).__init__(self, *args, **kwargs)
        self.guid = str(uuid.uuid4())
        self._callback_manager_lock = threading.Lock()
        self._callback_managers = {}
        self.subscription_queue = []

        self.after('connect', self.flush_callbacks)
        self.override('listen', self.run_master_dispatch)
        self.after('parse_message', self.dispatch_callbacks)
        self.before('subscribe', self.add_callback)
        self.override('subscribe', self.queue_subscribe)
        self.after('unsubscribe', self.remove_callbacks)


    def flush_callbacks(self, *args, **kwargs):
        """Subscribe to any subjects with registered callbacks"""
        for q_args, q_kwargs in list(self.subscription_queue):
            self.subscribe(*q_args, **q_kwargs)
        self.subscription_queue = []

    def run_master_dispatch(self, imethod, *args, **kwargs):
        """Run a blocking dispatch thread"""
        try:
            # Use a ThreadPoolExecutor to help catch exceptions
            with ThreadPoolExecutor(max_workers=1) as ex:
                ex.map(self.parse_message, imethod())
        except Exception as e:
            LOG.exception(e)

    def dispatch_callbacks(self, msg, *args, **kwargs):
        self.callback_manager.dispatch_callbacks(msg)

    def add_callback(self, subject, callback, *args, **kwargs):
        """Add callback to callback manager"""
        sync = kwargs.pop('sync', False)
        self.callback_manager.add_callback(subject, callback, sync)

    def remove_callbacks(self, _, subject, *args, **kwargs):
        self.callback_manager.remove_callbacks(subject)

    def queue_subscribe(self, imethod, *args, **kwargs):
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

    @abstractproperty
    def status(self):
        return '{} LISTENING'.format(self.guid)

    @abstractmethod
    def run(self, msg):
        """Define this in your subclass"""
        raise NotImplementedError


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
