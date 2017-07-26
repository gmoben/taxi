import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from os.path import abspath, basename, dirname, join, splitext
import sys
import tempfile

import docker

# Setup logging
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(funcName)s %(message)s')

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


PROJECT_ROOT = abspath(os.path.join(dirname(__file__), '..'))
PROJECT_NAME = basename(PROJECT_ROOT)


def find_dockerfiles(path):
    """ Recurse a path and return Dockerfile paths keyed by image name """
    files = {}

    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith('Dockerfile') and not filename.startswith('.'):
                key = dirpath[len(path):].lstrip('/').replace('/', '-')

                # Check for files like 'extract.Dockerfile' and append the prefix to the key
                if filename != 'Dockerfile':
                    suffix, _ = splitext(filename)
                    if suffix:
                        key += '-{}'.format(suffix)

                files[key] = join(dirpath, filename)

    return files


def build_tag(project_name, image_name, suffix=None):
    tag = '{}/{}'.format(project_name, image_name)
    if suffix:
        tag = '{}:{}'.format(tag, suffix)
    return tag


def build_image(client, image_name, df=None, **kwargs):

    # Define overridable defaults
    build_kwargs = {}
    build_kwargs['path'] = PROJECT_ROOT
    build_kwargs['decode'] = True
    build_kwargs['rm'] = True # Remove intermediate containers

    dockerfile = kwargs.get('dockerfile', df)
    if dockerfile:
        build_kwargs['dockerfile'] = dockerfile
        LOG.info('Building %s', dockerfile)
    else:
        LOG.info('Building %s from fileobj', image_name)

    tag_suffix = kwargs.get('tag_suffix', 'latest')
    tag = build_tag(PROJECT_NAME, image_name, tag_suffix)
    build_kwargs['tag'] = tag

    build_kwargs.update(kwargs)

    LOG.debug('Build image kwargs %s', build_kwargs)

    build_stream = client.api.build(**build_kwargs)

    for msg in build_stream:
        if 'stream' in msg:
            LOG.info('%s %s', image_name, msg['stream'].strip())
        elif 'error' in msg:
            LOG.error('%s %s', image_name, msg['error'].strip())
            raise Exception(msg['error'])
        else:
            LOG.debug('%s %s', image_name, msg)

    return tag


def run_docker(client, network, image, name, module=None, classname=None,
               cmd=None, detach=True, debug=False, threads=None, **kwargs):

    networks = [network] if not hasattr(network, '__iter__') else network

    if not cmd:
        if module and classname:
            cmd = 'python /opt/scripts/run_worker.py {} {}'.format(module, classname)
            if debug:
                cmd += ' -d'
            if threads:
                cmd += ' -t {}'.format(threads)
        else:
            LOG.warning('Running %s without a command', name)

    volumes = {
        'scripts': '/opt/scripts',
        'bus': '/opt/pylibs/bus',
        'plugins': '/opt/pylibs/plugins',
        'test/data/audio': '/opt/audio',
        'test/data/models': '/opt/svar/models'
    }

    volumes = {os.path.join(PROJECT_ROOT, k): {'bind': v, 'mode': 'ro'} for k, v in volumes.items()}

    image_tag = build_tag(PROJECT_NAME, image, 'latest')

    #links = {'nats-main': 'nats'}

    return client.containers.run(image_tag, cmd, name=name, networks=networks,
                                 detach=detach, volumes=volumes, **kwargs)


def kill_all(client):
    for x in client.containers.list(all=True):
        try:
            x.kill()
        except docker.errors.APIError:
            LOG.warning("%s wasn't alive to kill", x.name)
        x.remove()


def log_everything(containers, wait=False):

    containers = [containers] if not hasattr(containers, '__iter__') else containers

    def listen(container):
        for msg in container.logs(stdout=True, stderr=True, stream=True):
            LOG.info('%s: %s', container.name, msg.decode('utf-8').rstrip())

    with ThreadPoolExecutor(max_workers=len(containers)) as e:
        jobs = []
        for c in containers:
            job = e.submit(listen, c)
            done_msg = '{} is done logging'.format(c.name)
            job.add_done_callback(lambda x: LOG.info(done_msg))
            jobs.append(job)
        if wait:
            concurrent.futures.wait(jobs, return_when=concurrent.futures.FIRST_EXCEPTION)


def build_test_image(client):
    paths = find_dockerfiles(PROJECT_ROOT).values()

    with tempfile.NamedTemporaryFile() as buf:

        buf.write(b'FROM continuumio/miniconda\n')

        for x in paths:
            with open(x, 'r') as f:
                lines = [bytearray(line, 'utf-8') for line in f if not line.startswith('FROM')]
                buf.writelines(lines)

        buf.write(b'RUN conda install pytest mock\n')

        buf.write(b'RUN wget -O /tmp/go1.8.tar.gz https://storage.googleapis.com/golang/go1.8.linux-amd64.tar.gz\n')
        buf.write(b'RUN tar -C /usr/local -xzf /tmp/go1.8.tar.gz\n')
        buf.write(b'RUN mkdir -p /opt/go\n')
        buf.write(b'ENV GOPATH=/opt/go\n')
        buf.write(b'ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin\n')

        buf.write(b'RUN go get github.com/nats-io/gnatsd\n')

        buf.write(b'ADD test /opt/test')

        buf.seek(0)

        return build_image(client, 'test', buf.name)


def run_tests():
    client = docker.from_env(version='1.26')

    tag = build_test_image(client)

    kill_all(client)

    try:
        container = run_docker(client, 'bridge', 'test', 'test',
                               detach=True,
                               #tty=True)
                               cmd='py.test -vvv /opt/test')
        log_everything([container], wait=True)
    finally:
        pass
        #kill_all(client)
