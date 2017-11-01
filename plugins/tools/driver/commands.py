# TODO: Remove this module

from os.path import abspath, dirname, join

from taxi.util import StringTree, load_yaml


def _get_path():
    # TODO: Retrieve all subjects from plugins
    directory = dirname(__file__)
    filepath = join(directory, 'commands.yaml')
    return abspath(filepath)


for k, v in load_yaml(_get_path()).items():
    globals()[k.upper()] = StringTree(k, v)
