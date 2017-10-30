import time
import uuid
from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import ThreadPoolExecutor

import six
import structlog

from taxi.core.callback import Dispatcher
from taxi.util import (
    Wrappable,
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

    def pattern_match(self, pattern, subject):
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
            if self.pattern_match(pattern, subject):
                log.debug('Matching pattern found', pattern=pattern)
                self.dispatcher.dispatch(pattern, msg, *args, **kwargs)

    def register_callback(self, pattern, callback, *_, **__):
        self.dispatcher.register(pattern, callback)

    def unregister_callback(self, _, subject, *__, **___):
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
