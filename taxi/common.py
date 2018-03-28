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
    config_path = os.getenv('TAXI_CONFIG')
    if config_path:
        config = ConfigObj(load_yaml(config_path))
        LOG.info('Loaded config', config=config, path=config_path)
    else:
        cwd = os.path.dirname(os.path.abspath(__file__))
        default_channels = os.path.join(cwd, 'config/channels.yaml')
        config = ConfigObj({
            'engine': 'nats',
            'host': 'nats',
            'port': 4222,
            'channels': default_channels
        })
except Exception as e:
    LOG.exception('Error loading config', exc_info=e)
    raise
