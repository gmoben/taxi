from threading import Thread
import json
import signal
import time

from taxi.mixins.ha import NodeMixin
from taxi.util import subtopic


class SensorMixin(NodeMixin):

    def setup(self):
        self.sensor = self.init_sensor()
        self._should_shutdown = False
        self._thread = Thread(target=self._run_loop)
        self._thread.daemon=True
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)
        self._thread.start()

    def shutdown(self):
        self._should_shutdown = True

    def _run_loop(self):
        topic = subtopic(self.NAMESPACE, self.guid)
        while not self._should_shutdown:
            try:
                try:
                    data = self.read()
                    self.log.debug('Read data', data=data)
                except Exception as e:
                    self.log.exception('Read failed', exc_info=e)
                else:
                    if isinstance(data, dict):
                        data = json.dumps(data)
                    self.publish(topic, data)
            finally:
                time.sleep(1)

    def read(self):
        raise NotImplementedError
