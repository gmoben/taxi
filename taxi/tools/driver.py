import sys

import structlog

from taxi import Worker, config
from taxi.util import StringTree

LOG = structlog.getLogger(__name__)

DRIVER = StringTree('driver', {'command': ['run']})


class Driver(Worker('driver')):
    pass



def main(count, arg):
    driver = Driver()

    for x in range(count):
        driver.publish(DRIVER.COMMAND.RUN, arg)
