from concurrent.futures import ThreadPoolExecutor
import time

from Adafruit_MCP9808.MCP9808 import MCP9808

from taxi import Worker


PART_NUMBER='MCP9808'


def c_to_f(c):
    """ Convert celsius to fahrenheit """
    return c * 9.0 / 5.0 + 32.0


class TemperatureWorker(Worker('temperature', PART_NUMBER)):

    def setup(self):
        self.sensor = self.init_sensor()
        ex = ThreadPoolExecutor(max_workers=1)
        ex.submit(self.run_loop)
        ex.shutdown(wait=False)

    @staticmethod
    def init_sensor():
        sensor = MCP9808()
        sensor.begin()
        return sensor

    def run_loop(self):
        while True:
            try:
                c_temp = self.readTemp()
                f_temp = c_to_f(c_temp)
                self.log.info('Read temp', celsius=c_temp, fahrenheit=f_temp)
                self.publish('temp.celsius', str(c_temp))
                self.publish('temp.fahrenheit', str(f_temp))
            finally:
                time.sleep(1)

    def readTemp(self, format='C'):
        temp = self.sensor.readTempC()
        if format == 'C':
            return temp
        elif format == 'F':
            return c_to_f(temp)
        else:
            raise Exception
