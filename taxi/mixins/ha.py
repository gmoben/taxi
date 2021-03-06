import structlog

from taxi.mixins import ClientMixin

from taxi.util import subtopic, StringTree

HA = StringTree('ha', ['work', 'status'])


class NodeMixin(ClientMixin):
    """Convenience class that executes ``self.setup`` then automatically connects"""
    NAMESPACE = None

    def __init__(self, *args, **kwargs):
        super(NodeMixin, self).__init__(self, *args, **kwargs)

        self.log = structlog.getLogger(self.__class__.__name__).bind(namespace=self.NAMESPACE)
        self.log.info('Starting node')

        self.setup()
        self.connect()

    def setup(self):
        """Setup the instance prior to connecting (subscribing and setting up callbacks, etc.)"""
        pass


class ManagerMixin(NodeMixin):

    def poll_workers(self, on_response, timeout=5):
        """Send a status request to workers in this namespace.

        :param timeout: Length of time to wait for responses
        :returns: Parsed response messages
        :rtype: list

        """
        self.request(subtopic(HA.STATUS, self.NAMESPACE), 'poll', on_response, timeout)

    def publish_work(self, data, worker_id=None):
        """Publish work data.

        If ``worker_id`` is defined, send data to a specific worker without broadcasting.

        :param string data: Data to deliever
        :param string worker_id: Specific worker ID to recieve data (don't broadcast)

        """
        if worker_id:
            self.publish(subtopic(HA.WORK, self.NAMESPACE, worker_id), data)
        else:
            self.publish(subtopic(HA.WORK, self.NAMESPACE), data)


class WorkerMixin(NodeMixin):
    """Worker node acting on work in its NAMESPACE"""

    def __init__(self, *args, **kwargs):
        """Initialize the worker and subscribe to work channels for the namespace"""
        super(WorkerMixin, self).__init__(*args, **kwargs)
        self.after('connect', self._subscribe_ha)
        self._subscribe_ha()

    def _subscribe_ha(self, *args, **kwargs):
        self.subscribe(subtopic(HA.WORK, self.NAMESPACE), self.on_work, queue_group=self.NAMESPACE)
        self.subscribe(subtopic(HA.WORK, self.NAMESPACE, str(self.guid)), self.on_work)
        self.subscribe(subtopic(HA.STATUS, self.NAMESPACE), lambda msg: self.publish(msg.meta.reply_to, self.get_status()))

    def get_status(self):
        return '{} LISTENING'.format(self.guid)

    def on_work(self, msg):
        """Define this in your subclass"""
        pass
