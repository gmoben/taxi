import os

from configobj import ConfigObj
import structlog

from taxi.util import load_yaml


LOG = structlog.getLogger('taxi')


try:
    config_path = os.environ['TAXI_CONFIG']
    config = ConfigObj(load_yaml(config_path))
except KeyError:
    raise RuntimeError("Required environment variable not set")
except:
    LOG.exception()
    raise


LOG = LOG.bind(engine=config['engine'])
