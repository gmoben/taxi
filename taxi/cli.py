import argparse
import importlib

from taxi.tools.driver import main as run_driver
import structlog

LOG = structlog.getLogger()


def get_args():

    parser = argparse.ArgumentParser('Taxi CLI')
    parser.add_argument('--debug', '-d', action='store_true', help='Use debug logging')

    parser.add_argument('cmd', type=str, help='command to run')
    parser.add_argument('cmd_args', type=str, help='arguments to command', nargs='+')

    return parser.parse_args()


def start(module, classname):
    mod = importlib.import_module(module)
    cls = getattr(mod, classname)
    if cls:
        worker = cls()
        worker.listen()

def test(count, arg):
    run_driver(int(count), arg)

def main():
    args = get_args()
    try:
        func = globals()[args.cmd]
    except KeyError:
        LOG.error('Command not found', command=args.cmd)
    func(*args.cmd_args)


if __name__ == '__main__':
    main()
