from abc import ABCMeta, abstractmethod, abstractproperty

import six

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

    @staticmethod
    def pattern_match(pattern, subject):
        """Determine if a given subject pattern matches a string.

        Defaults to performing an equality comparison between the parameters.

        :param string pattern: Pattern to match
        :param string string: String to match
        :returns: Whether or not the pattern matches the string
        :rtype: boolean

        """
        return pattern == subject

    @staticmethod
    def get_subtopic_pattern(subject, shallow):
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
