import argparse
import importlib
import logging
import sys

def get_args():
    parser = argparse.ArgumentParser('Start a worker node process')
    parser.add_argument('module', type=str, help='module to import')
    parser.add_argument('classname', type=str, help='class name to start')
    parser.add_argument('--debug', '-d', action='store_true', help='Use debug logging')
    parser.add_argument('--host', type=str, default='nats', required=False,
                        help='Hostname of NATS Server')
    parser.add_argument('--port', '-p', type=int, default='4222', required=False,
                        help='Port of NATS Server')
    parser.add_argument('--max_workers', '--max_threads', '-t', type=int, default=None, required=False,
                        help='Max number of concurrent callback handlers for the client')

    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(stream=sys.stdout, level=log_level)

    mod = importlib.import_module(args.module)
    cls = getattr(mod, args.classname)
    if cls:
        worker = cls(host=args.host, port=args.port, max_workers=args.max_workers)
        worker.listen()
