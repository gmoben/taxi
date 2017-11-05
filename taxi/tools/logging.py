from pprint import pformat

import structlog

from taxi import Worker
from taxi.util import subtopic


LOG = structlog.getLogger(__name__)


_LogWorker = Worker('logging')


class MessageLogger(_LogWorker):

    def setup(self):
        self.subscribe('>', self.on_msg)

    def on_msg(self, msg):
        """ Print truncated message """
        LOG.info(pformat(str(msg)[:1024]))
