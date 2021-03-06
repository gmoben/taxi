from taxi.common import config
from taxi.util import StringTree, load_yaml

for k, v in load_yaml(config['channels']).items():
    globals()[k.upper()] = StringTree(k, v)
