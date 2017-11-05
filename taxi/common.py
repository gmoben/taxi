import logging
import os

from configobj import ConfigObj
import structlog

from taxi.util import load_yaml

structlog.configure_once(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)


logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO').upper())
LOG = structlog.getLogger()


try:
    config_path = os.environ['TAXI_CONFIG']
    config = ConfigObj(load_yaml(config_path))
    LOG.info('Loaded config', config=config, path=config_path)
except KeyError:
    raise RuntimeError("Required environment variable not set")
except:
    LOG.exception('Error loading config')
    raise
