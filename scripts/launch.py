import concurrent.futures
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from os.path import abspath, basename, dirname
from pprint import pformat

import docker

from util import run_docker as run
from util import build_image, find_dockerfiles, kill_all, log_everything

# Setup logging
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(funcName)s %(message)s')

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

# Assuming this file lives in PROJECT_NAME/scripts
PROJECT_ROOT = abspath(os.path.join(dirname(__file__), '..'))
PROJECT_NAME = basename(PROJECT_ROOT)


def subtopic(*args):
    return '.'.join([str(x) for x in args])


def build_all(client, parallel=True):
    # Build the base image first
    dockerfiles = find_dockerfiles(PROJECT_ROOT)

    if not PROJECT_NAME in dockerfiles:
        raise RuntimeError('Missing base image')

    # TODO: Don't depend on project structure to find the base image
    build_image(client, 'base', dockerfiles[PROJECT_NAME])
    del dockerfiles[PROJECT_NAME]

    # Build the rest of the images

    if not len(dockerfiles):
        return

    max_workers = len(dockerfiles) if parallel else 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        args = [(client, image_name, filepath) for image_name, filepath in dockerfiles.items()]
        pool.map(lambda x: build_image(*x), args)


def run_loggers(client, network):
    module = 'plugins.tools.logging'
    return [
        run(client, network, 'base', 'msglog',
            subtopic(module, 'workers'), 'MessageLogger')
    ]


def wait_for_ips(client, network, timeout=5):
    containers = client.containers.list()
    LOG.info('Waiting on IPs for %s', [x.name for x in containers])

    def _ip(container):
        return container.attrs['NetworkSettings']['Networks'][network]['IPAddress']

    def wait_for_ip(container):
        ip = None
        while not ip:
            ip = _ip(client.containers.get(container.id))
        return ip, container.name

    with ThreadPoolExecutor(max_workers=len(containers)) as e:
        ips = [e.submit(wait_for_ip, x) for x in containers]

    done, not_done = concurrent.futures.wait(ips, timeout=timeout)
    return sorted([x.result() for x in done], key=lambda x: x[0])


def run_all(client, network='bridge'):

    # Start nats
    gnatsd = client.containers.run('nats', name='nats-main', network=network, detach=True)
    wait_for_ips(client, network)

    containers = [gnatsd]
    loggers = run_loggers(client, network)

    ips = wait_for_ips(client, network)

    LOG.info('Container IPs:\n%s', pformat(ips))

    # time.sleep(10)
    containers += loggers
    log_everything(containers, wait=True)


def main():
    client = docker.from_env(version='1.26')

    build_all(client)
    kill_all(client)
    run_all(client)


if __name__ == '__main__':
    main()
