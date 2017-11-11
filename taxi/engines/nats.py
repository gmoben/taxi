import fnmatch
import json
import socket
import telnetlib
import threading

import six

from taxi.abstract import AbstractEngine
from taxi.util import (
    AttrDict,
    callable_fqn as fqn,
    threadsafe_defaultdict as defaultdict
)


class ConcreteEngine(AbstractEngine):
    """NATS client implementation based on http://nats.io/documentation/internals/nats-protocol/"""

    def __init__(self, *args, **kwargs):
        self._telnet = None
        self._telnet_connected = False
        self._last_sid = -1
        self._sid_lock = threading.Lock()
        self._subscription_ids = {}
        self._write_lock = threading.Lock()
        super(ConcreteEngine, self).__init__(*args, **kwargs)

    def __del__(self):
        """ Close the connection when the instance is garbage collected """
        telnet = getattr(self, '_telnet', None)
        if telnet and hasattr(self, 'disconnect'):
            self.disconnect()

    def _get_sid(self, channel):
        """ Get a new SID starting from 0 for this connection"""
        with self._sid_lock:
            try:
                sid = self._subscription_ids[channel]
            except KeyError:
                self._last_sid += 1
                self._subscription_ids[channel] = self._last_sid
                sid = self._last_sid
        return sid

    def _remove_sid(self, channel):
        with self._sid_lock:
            try:
                sid = self._subscription_ids[channel]
                del self._subscription_ids[channel]
            except KeyError:
                sid = None
        return sid

    def _write(self, msg):
        """Deliever the message to the NATS Server via telnet.

        :param msg: raw message to send
        :type msg: bytestring or utf-8 string
        """

        log = self.log.bind(message=msg)

        try:
            if six.PY3:
                msg = bytearray(msg, "utf-8")

            if self.connected:
                self._write_lock.acquire()
                self._telnet.write(msg)
                self._write_lock.release()
                log.debug('Message sent')
                return True
            else:
                log.error('Message not sent', reason='telnet disconnected')
                return False
        except:
            self.log.exception('Unhandled exception during message write')
            return False

    def _write_connect(self, options=None):
        """Send a NATS specific CONNECT message"""
        options = options or {}
        return self._write('CONNECT {}\r\n'.format(json.dumps(options)))

    def connect(self, host=None, port=None):
        host = host or self.host
        port = port or self.port
        if host is None or port is None:
            self.log.error('Missing host or port')
            return False

        self.host = host
        self.port = port
        self.log = self.log.bind(host=host, port=port)

        try:
            self.log.debug('Establishing telnet connection')
            self._telnet = telnetlib.Telnet(host, port)
            self._telnet_connected = True
            self.log = self.log.bind(connected=True)
            self.log.debug('Telnet connection established')
            return True
        except socket.error:
            self.log.exception('Telnet connection failed')
        except:
            self.log.exception('Unhandled exception during connect')
        return False

    @property
    def connected(self):
        return self._telnet_connected

    @connected.setter
    def connected(self, value):
        self._telnet_connected = value
        if value is False and self.attempt_reconnect:
            self.log = self.log.bind(connected=False)
            for _ in range(5):
                self.log.info('Attempting to reconnect')
                self.connect()
            if not self._telnet_connected:
                self.log.error('Unable to reconnect')

    def listen(self):
        if not self.connected:
            self.connect()

        self.log.info('Listening')

        delimiter = bytearray('\r\n', 'utf-8')
        while self.connected:
            try:
                msg = self._telnet.read_until(delimiter).decode('utf-8')
                if msg.startswith('MSG'):
                    # Append the data
                    msg += self._telnet.read_until(delimiter).decode('utf-8')
                self.log.debug('Message recieved', message=msg)
                yield msg
            except EOFError:
                self.log.exception('No more messages')
                self.disconnect()
            except GeneratorExit:
                self.log.debug('GeneratorExit during listen')
                self.disconnect()
            except:
                self.log.exception('Unhandled exception while listening')
                self.disconnect()
        self.log.info('Stopped listening')
        yield None

    def disconnect(self):
        self.log.info('Closing connection')
        if self._telnet and self._telnet.sock:
            self._telnet.close()
            self._telnet_connected = False
            self.log.info('Telnet connection closed')
            self.log = self.log.bind(connected=False)
            return True
        self.log.warning('Telnet already disconnected')
        return False

    def parse_message(self, msg):
        meta = AttrDict(op=None, sid=None, reply_to=None)
        parsed_msg = AttrDict(channel='', meta=meta, data=None)

        if msg is None:
            self.log.error('Message was NoneType', msg=msg)
            return parsed_msg

        msg = msg.strip()
        op, _, body = msg.partition(' ')

        meta.op = op

        log = self.log.bind(op=op)
        if op == 'PING':
            self.pong()
        elif op == 'INFO':
            parsed_msg.data = json.loads(body)
        elif op in ['+OK', '-ERR']:
            parsed_msg.data = body
        elif op == 'MSG':
            # Split between header and data
            split_msg = body.strip().split('\r\n')

            # Add empty string as data portion
            if len(split_msg) == 1 and split_msg[0].endswith('0'):
                split_msg += ['']

            # Abort if no data
            if len(split_msg) != 2:
                log.error('Invalid message format', split_msg=split_msg)
                return None

            # Split header and remove bytes_count
            header = split_msg[0].split()[0:-1]
            data = split_msg[1]

            # Abort if invalid message
            if len(header) not in [2, 3]:
                log.error('Invalid message header')
                return None

            fields = ['channel', 'sid', 'reply_to']
            if len(header) == 2:
                # Set reply_to to None
                header.append(None)
            fields = AttrDict(zip(fields, header))
            meta.sid = fields.sid
            meta.reply_to = fields.reply_to
            parsed_msg.channel = fields.channel
            parsed_msg.data = data
            log.debug('Message parsed', parsed_message=parsed_msg)
        else:
            log.warning('Unknown OP', message=msg)

        if parsed_msg.data is None:
            parsed_msg.data = body or msg

        return parsed_msg

    @staticmethod
    def pattern_match(pattern, channel):
        if pattern == channel:
            return True

        if pattern.endswith('>'):
            if channel.startswith(pattern[:-1]):
                return True
            else:
                # Replace > with * for glob matching step below
                pattern = '{}*'.format(pattern[:-1])

        if '*' in pattern and fnmatch.fnmatchcase(channel, pattern):
            # Pattern matches * wildcard
            return True
        return False

    def get_subtopic_pattern(self, channel, shallow=True):
        if shallow:
            suffix = '.*'
        else:
            suffix = '.>'

        return '{}{}'.format(channel, suffix)

    def publish(self, channel, data, reply_to=None, wait=False):
        # TODO: Implement wait
        self.log.debug('Publish', channel=channel, data=data, reply_to=reply_to, wait=wait)

        args = [str(x) for x in [channel, reply_to, len(data)] if x is not None]
        return self._write('PUB {}\r\n{}\r\n'.format(' '.join(args), data))

    def subscribe(self, channel, callback=None, queue_group=None, sync=False, wait=False):
        """Subscribe to a channel.

        :param string queue_group: If specified, join this queue group

        """
        # If the channel already has an active subscription, reuse the first SID
        sid = self._get_sid(channel)
        self.log.debug('Subscribe', channel=channel, sid=sid, queue_group=queue_group,
                       sync=sync, callback=fqn(callback), wait=wait)
        args = [str(x) for x in [channel, queue_group, sid] if x is not None]
        return self._write('SUB {}\r\n'.format(' '.join(args)))

    def unsubscribe(self, channel, max_msgs=None):
        """Unsubscribe from a channel.

        :param int max_msgs: Number of messages to wait for before
            automatically unsubscribing

        """
        sid = self._remove_sid(channel)
        log = self.log.bind(channel=channel, sid=sid, max_msgs=max_msgs)
        if sid is None:
            log.error('Subscription ID not found', subscription_ids=self._subscription_ids)
            return False

        log.debug('Unsubscribe')
        args = [str(x) for x in [sid, max_msgs] if x is not None]
        return self._write('UNSUB {}\r\n'.format(' '.join(args)))

    def ping(self):
        self.log.debug('Ping')
        return self._write('PING\r\n')

    def pong(self):
        self.log.debug('Pong')
        return self._write('PONG\r\n')
