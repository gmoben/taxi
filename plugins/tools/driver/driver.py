import logging
import sys

from plugins.tools.driver import commands
from taxi.core import Worker


logging.basicConfig(stream=sys.stdout)
LOG = logging.getLogger(__name__)


def run(count, arg, host):
    worker = Worker('base-worker')(host='172.17.0.2')

    for x in range(count):
        worker.publish(commands.DRIVER.COMMANDS.RUN, arg)


if __name__ == '__main__':
    count = int(sys.argv[1])
    arg = sys.argv[2]

    try:
        host = sys.argv[3]
    except:
        host='nats'

    run(count, arg, host)
