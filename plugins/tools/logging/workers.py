import logging
import os
import sys
import time
from pprint import pformat

from taxi.core import Worker
from taxi.util import subtopic

logging.basicConfig(stream=sys.stdout)
LOG = logging.getLogger(__name__)


_LogWorker = Worker('logging')


class MessageLogger(_LogWorker):

    def setup(self):
        self.subscribe('>', self.on_msg)

    def on_msg(self, msg):
        """ Print truncated message """
        LOG.info(pformat(str(msg)[:1024]))
