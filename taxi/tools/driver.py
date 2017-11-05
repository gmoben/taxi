import sys

import structlog

from taxi import Worker, config
from taxi.util import StringTree

LOG = structlog.getLogger(__name__)

DRIVER = StringTree('driver', {'command': ['run']})


class Driver(Worker('driver')):
    pass


def run(count, arg):
    driver = Driver()

    for x in range(count):
        driver.publish(DRIVER.COMMAND.RUN, arg)


if __name__ == '__main__':
    count = int(sys.argv[1])
    arg = sys.argv[2]
    run(count, arg)
