import base64
import fnmatch
import json
import logging
import socket
import telnetlib
import threading
import time
from contextlib import contextmanager
from collections import defaultdict

import six
import pexpect

from bus.core.base import AbstractEngine

log = logging.getLogger(__name__)


@contextmanager
def server():
    """ Use a gnatsd client in a context """
    process = pexpect.spawn('gnatsd')
    #process.expect('[INF] Server is ready')
    time.sleep(1)
    yield process
    process.kill(15)


class ConcreteEngine(AbstractEngine):
    """ NATS client implementation based on http://nats.io/documentation/internals/nats-protocol/ """

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
        if telnet and hasattr(self, 'close'):
            self.close()

    def _get_sid(self, subject):
        """ Get a new SID starting from 0 for this connection"""
        with self._sid_lock:
            try:
                sid = self._subscription_ids[subject]
            except KeyError:
                self._last_sid += 1
                self._subscription_ids[subject] = self._last_sid
                sid = self._last_sid
        return sid

    def _remove_sid(self, subject):
        with self._sid_lock:
            try:
                sid = self._subscription_ids[subject]
                del self._subscription_ids[subject]
            except KeyError:
                sid = None
        return sid

    def _write(self, msg):
        """Deliever the message to the NATS Server via telnet.

        :param msg: raw message to send
        :type msg: bytestring or utf-8 string
        """

        if six.PY3:
            msg = bytearray(msg, "utf-8")

        if self.connected:
            self._write_lock.acquire()
            self._telnet.write(msg)
            self._write_lock.release()
            log.debug('Sent: %s', msg)
        else:
            log.error('Unable to deliever message (telnet client is not connected).')

    def _write_connect(self, options=None):
        """Send a NATS specific CONNECT message"""
        options = options or {}
        self._write('CONNECT {}\r\n'.format(json.dumps(options)))

    def connect(self, host='0.0.0.0', port=4222):
        try:
            host = self.host or host
            port = self.port or port
            self._telnet = telnetlib.Telnet(host, port)
            self._telnet_connected = True
        except socket.error:
            log.exception('Unable to connect to %s:%s', self.host, self.port)
            raise
        except Exception as e:
            log.exception(e)
            raise

    @property
    def connected(self):
        return self._telnet_connected

    @connected.setter
    def connected(self, value):
        self._telnet_connected = value
        if value is False and self.attempt_reconnect:
            for _ in range(5):
                log.info('Attempting to reconnect to %s:%s', self.host, self.port)
                self.connect()
            if not self._telnet_connected:
                log.error('Unable to reconnect to %s:%s')

    def listen(self):
        if not self.connected:
            self.connect()

        log.info('Listening')

        delimiter = bytearray('\r\n', 'utf-8')
        while self.connected:
            try:
                msg = self._telnet.read_until(delimiter).decode('utf-8')
                if msg.startswith('MSG'):
                    # Append the payload
                    msg += self._telnet.read_until(delimiter).decode('utf-8')
                yield msg
            except EOFError:
                log.exception('No more messages')
                self.close()
            except:
                log.exception('Unhandled exception while listening')
                self.close()
        log.info('Stopped listening')
        yield None

    def close(self):
        log.info('Closing connection')
        if self._telnet and self._telnet.sock:
            self._telnet.close()
            self._telnet_connected = False
            return True
        return False

    def parse_message(self, msg):
        log.debug('Received: %s', msg.strip())
        parsed_msg = super(ConcreteEngine, self).parse_message(msg)
        if msg.startswith('PING'):
            self.pong()
        elif msg.startswith('MSG'):
            # Split between header and payload
            split_msg = msg.strip().split('\r\n')

            # Abort if no payload
            if len(split_msg) != 2:
                log.error('Unable to parse message (invalid message format): %s', msg)
                return None

            # Split header and remove MSG and bytes_count
            header = split_msg[0].split()[1:-1]
            payload = split_msg[1]

            # Abort if invalid message
            if len(header) not in [2, 3]:
                log.error('Unable to parse message (invalid message header): %s', msg)
                return None

            fields = ['subject', 'sid', 'reply_to']
            if len(header) == 2:
                # Set reply_to to None
                header.append(None)
            parsed_msg = dict(zip(fields, header))
            parsed_msg['payload'] = payload

            log.debug('Received message from %s (SID %s, reply_to: %s)', *header)

        return parsed_msg

    def matches_subject(self, pattern, subject):
        if pattern == subject:
            return True

        if pattern.endswith('>'):
            if subject.startswith(pattern[:-1]):
                return True
            else:
                # Replace > with * for glob matching step below
                pattern = '{}*'.format(pattern[:-1])

        if '*' in pattern and fnmatch.fnmatchcase(subject, pattern):
            # Pattern matches * wildcard
            return True
        return False

    def get_subtopic_pattern(self, subject, shallow=True):
        if shallow:
            suffix = '.*'
        else:
            suffix = '.>'

        return '{}{}'.format(subject, suffix)

    def publish(self, subject, payload, wait=False, reply_to=None):
        # TODO: Implement wait
        log.debug('Publishing to %s (reply_to: %s)', subject, reply_to)

        args = [str(x) for x in [subject, reply_to, len(payload)] if x is not None]
        self._write('PUB {}\r\n{}\r\n'.format(' '.join(args), payload))

    def subscribe(self, subject, callback=None, wait=False, queue_group=None, sync=False):
        """Subscribe to a subject.

        :param string queue_group: If specified, join this queue group

        """
        # If the subject already has an active subscription, reuse the first SID
        sid = self._get_sid(subject)
        log.debug('Subscribing to %s (SID %s)', subject, sid)
        args = [str(x) for x in [subject, queue_group, sid] if x is not None]
        self._write('SUB {}\r\n'.format(' '.join(args)))

    def unsubscribe(self, subject, max_msgs=None):
        """Unsubscribe from a subject.

        :param int max_msgs: Number of messages to wait for before
            automatically unsubscribing

        """
        sid = self._remove_sid(subject)
        if sid is None:
            log.exception('No sid found for subject %s', subject)
            return

        log.debug('Unsubscribing from %s (sid: %s max_msgs: %s)', subject, sid, max_msgs)
        args = [str(x) for x in [sid, max_msgs] if x is not None]
        self._write('UNSUB {}\r\n'.format(' '.join(args)))

    def ping(self):
        self._write('PING\r\n')

    def pong(self):
        self._write('PONG\r\n')
