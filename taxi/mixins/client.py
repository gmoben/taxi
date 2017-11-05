from abc import ABCMeta
from concurrent.futures import ThreadPoolExecutor
import time
import uuid

import six
import structlog

from taxi.dispatch import Dispatcher
from taxi.util import Wrappable, subtopic


LOG = structlog.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class ClientMixin(Wrappable):
    """Mixin for AbstractEngine with callback management and convenience methods"""

    def __init__(self, *args, **kwargs):
        super(ClientMixin, self).__init__(self, *args, **kwargs)
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
        for pattern, group in self.dispatcher.groups.items():
            if self.pattern_match(pattern, subject):
                log.debug('Matching pattern found', pattern=pattern)
                group.dispatch(*args, **kwargs)

    def register_callback(self, pattern, callback, sync=False, label=None, max_workers=None):
        label = label or ('sync' if sync is True else ('async' if sync is False else label))
        max_workers = 1 if sync else max_workers
        if not pattern in self.dispatcher.groups:
            group = self.dispatcher.init_group(pattern)
        else:
            group = self.dispatcher.groups[pattern]

        if not label in group.executors:
            group.init_executor(label, max_workers)

        return group.register(callback, label)

    def unregister_callbacks(self, _, pattern, *args, **kwargs):
        try:
            group = self.dispatcher.groups[pattern]
        except KeyError:
            self.log.error('Pattern has no callbacks', pattern=pattern)
            return False

        return group.unregister_all(remove_executors=True, wait=False)

    def queue_subscription(self, imethod, *args, **kwargs):
        """Queue a subscription if not currently connected"""
        if self.connected:
            return imethod(*args, **kwargs)
        return self.subscription_queue.append((args, kwargs))

    def request(self, subject, payload, callback, timeout):
        reply_to = subtopic('INBOX', self.guid)

        self.subscribe(reply_to, callback)
        self.publish(subject, payload, reply_to=reply_to)

        time.sleep(timeout)
        self.unsubscribe(reply_to)
