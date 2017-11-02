import structlog

from taxi.mixins import ClientMixin

from taxi.util import subtopic

LOG = structlog.getLogger(__name__)


class NodeMixin(ClientMixin):
    """Convenience class that executes ``self.setup`` then automatically connects"""
    NAMESPACE = None

    def __init__(self, *args, **kwargs):
        super(NodeMixin, self).__init__(self, *args, **kwargs)

        LOG.info('Starting %s [%s]', self.__class__.__name__, self.NAMESPACE)

        self.setup()
        self.connect()

    def setup(self):
        """Setup the instance prior to connecting (subscribing and setting up callbacks, etc.)"""
        pass


class ManagerMixin(NodeMixin):

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


class WorkerMixin(NodeMixin):
    """Worker node acting on work in its NAMESPACE"""

    def __init__(self, *args, **kwargs):
        """Initialize the worker and subscribe to work subjects for the namespace"""
        super(WorkerMixin, self).__init__(*args, **kwargs)

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
