from abc import ABCMeta, abstractmethod, abstractproperty

import six

from taxi.common import LOG, config
from taxi.util import threadsafe_defaultdict as defaultdict


@six.add_metaclass(ABCMeta)
class AbstractEngine(object):
    """Implement this and pass the subclass into a Factory method"""

    def __init__(self, host=None, port=None, attempt_reconnect=True, *args, **kwargs):
        """Engine initialization.

        :param string host: Server hostname/IP
        :param string port: Server port
        :param boolean attempt_reconnect: Attempt reconnection to server if lost

        """
        self.host = host or config['host']
        self.port = port or config['port']
        self.log = LOG.bind(connected=False)
        self.attempt_reconnect = attempt_reconnect
        super(AbstractEngine, self).__init__(*args, **kwargs)

    @abstractmethod
    def connect(self, host=None, port=None):
        """Connect to the server instance

        :param string host: Server hostname/IP
        :param string port: Server port
        :returns: connection status
        :rtype: boolean
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
        :returns: Parsed message containing the keys 'channel', 'data', and 'meta'

        """
        raise NotImplementedError

    @abstractmethod
    def publish(self, channel, data, wait=False, **options):
        """Publish a data to a channel.

        :param string channel: The destination channel
        :param string data: UTF-8 encoded message data
        :param boolean wait: Wait for reciept from server
        :param **options: Any additional options for the implementation

        :returns: Success status
        :rtype: boolean

        """
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, channel, callback, wait=False, **options):
        """Subscribe to a channel literally.

        :param string channel: The channel name to subscribe to
        :param function callback: Callback to execute when a message
            is received from this channel
        :param boolean wait: Wait for reciept from server
        :param **options: Any additional options for the implementation

        :return: Unique identifier of the resulting subscription
        :rtype: string

        """
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self, channel):
        """Unsubscribe by subscription id.

        :param string channel: The channel pattern to unsubscribe from

        """
        raise NotImplementedError

    @staticmethod
    def pattern_match(pattern, channel):
        """Determine if a given channel pattern matches a string.

        Defaults to performing an equality comparison between the parameters.

        :param string pattern: Pattern to match
        :param string string: String to match
        :returns: Whether or not the pattern matches the string
        :rtype: boolean

        """
        return pattern == channel

    @staticmethod
    def get_subtopic_pattern(channel, shallow):
        """Build a pattern for matching subtopics of a channel.

        :param channel: The base channel
        :param boolean shallow: If set, build channel matching direct children.
            Otherwise build a channel matching all descendents of a channel.
        :returns: The subtopic channel
        :rtype: string

        """
        return '.'.join([channel, '*'])

    def subscribe_subtopics(self, channel, callback, shallow=True):
        """Subscribe to subtopics of a channel.

        :param string channel: root channel
        :param function callback: Callback to execute when receiving a message
            from any subtopic
        :param boolean shallow: If set, only subscribe to direct children.
            Otherwise subscribe to all descendents of a channel.

        """
        channel = self.get_subtopic_pattern(channel, shallow)
        self.subscribe(channel, callback)
