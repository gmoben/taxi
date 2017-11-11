from threading import Thread
import json
import signal
import time

from Adafruit_MCP9808.MCP9808 import MCP9808

from taxi import ConcreteEngine, Sensor
from taxi.mixins import NodeMixin
from taxi.util import subtopic


def c_to_f(c):
    """ Convert celsius to fahrenheit """
    return c * 9.0 / 5.0 + 32.0

PART_NUMBER = 'MCP9808'
class TemperatureSensor(Sensor('temperature')):

    @staticmethod
    def init_sensor():
        sensor = MCP9808()
        sensor.begin()
        return sensor

    def read(self):
        temp = self.sensor.readTempC()
        return dict(
            part=PART_NUMBER,
            guid=self.guid,
            celsius=temp,
            fahrenheit=c_to_f(temp)
        )
