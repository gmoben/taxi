import os

from configobj import ConfigObj
import structlog

from taxi.util import load_yaml


LOG = structlog.getLogger()


try:
    config_path = os.environ['TAXI_CONFIG']
except KeyError:
    LOG.exception()
    raise RuntimeError("Environment variable 'TAXI_CONFIG' is not set")
config = ConfigObj(load_yaml(config_path))


LOG = LOG.bind(engine=config['engine'])
