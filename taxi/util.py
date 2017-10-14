import logging
import threading
import imp
import importlib
import os
from collections import defaultdict, deque, namedtuple
from contextlib import contextmanager

import six
import yaml

LOG = logging.getLogger(__name__)


def load_yaml(filename):
    with open(filename, 'r') as f:
        return yaml.load(f)


def dict_to_namedtuple(dictionary, name='GenericDict'):
    return namedtuple(name, dictionary.keys())(**dictionary)


def nameddict(dict_or_name=None, **kwargs):
    if isinstance(dict_or_name, dict):
        dictionary = dict_or_name
        name = 'GenericDict'
    else:
        name = dict_or_name or 'GenericDict'
        dictionary = dict(**kwargs)
    return dict_to_namedtuple(dictionary, name)


def callable_fqn(method):
    if type(method).__name__ in ['instancemethod', 'method']:
        return '.'.join([method.__module__, method.__self__.__class__.__name__, method.__name__])
    elif type(method).__name__ == 'function':
        return '.'.join([method.__module__, method.__name__])


class threadsafe_defaultdict(defaultdict):

    def __init__(self, *args, **kwargs):
        defaultdict.__init__(self, *args, **kwargs)
        self._lock = threading.Lock()

    def __getitem__(self, key):
        if key in self:
            return super(self, defaultdict).__getitem__(key)
        else:
            with self._lock:
                if key in self:
                    # Value for key was created whilst this thread was waiting for the lock
                    return super(self, defaultdict).__getitem__(key)
                else:
                    return self.__missing__(key)


class Wrappable(object):
    """Mixin for specifying instance methods that must always run
    around another instance method.

    The function decorators ``@self.before`` and ``@self.after``
    invert control and give superclasses the ability to specify
    methods that should always run before or after another method,
    regardless if the wrapped method is overridden in an subclass
    implementation.

    This is useful for defining control logic for abstract methods
    without depending on the subclass implementation to explicitly
    call the method on the superclass.
    """

    def __init__(self, *args, **kwargs):
        self._wrapper_map = defaultdict(lambda: defaultdict(lambda: list()))

    def __getattribute__(self, name):
        """ Intercept attributes and execute callaround functions if they exist"""
        attr = super(Wrappable, self).__getattribute__(name)
        if callable(attr) and name in self._wrapper_map.keys():
            callarounds = self._wrapper_map[name]
            if callarounds:
                func = callarounds['override'] or attr
                @six.wraps(attr)
                def metawrapper(*args, **kwargs):
                    try:
                        LOG.debug('%s before -  %s', callable_fqn(attr), callarounds['before'])
                        [x(*args, **kwargs) for x in callarounds['before']]
                        LOG.debug('%s func %s', callable_fqn(attr), callable_fqn(func))
                        result = func(attr, *args, **kwargs) if func != attr else func(*args, **kwargs)
                        LOG.debug('%s after - %s', callable_fqn(attr), callarounds['after'])
                        [x(result, *args, **kwargs) for x in callarounds['after']]
                    except Exception as e:
                        LOG.exception('Unhandled exception in %s', self)
                return metawrapper
        return attr

    def _around(self, when, name, func):
        self._wrapper_map[name][when].append(func)

    def before(self, name, func):
        return self._around('before', name, func)

    def after(self, name, func):
        return self._around('after', name, func)

    def override(self, name, func):
        if self._wrapper_map[name]['override']:
            raise ValueError('Cannot override a method twice')

        self._wrapper_map[name]['override'] = func


class StringTree(object):
    """
    Represent a nested dictionary as a hierarchy of constants.

    Example:
       APP = StringTree('app', {'events': {'button': ['toggle'])

       APP == 'app'
       APP.EVENTS == 'app.events'
       APP.EVENTS.BUTTON.TOGGLE == 'fds.events.button.toggle'
    """


    def __init__(self, name, data=None, parent=None):
        self.name = name
        self.parent = parent

        if data is not None:
            if not isinstance(data, (list, set)):
                data = [data]
            for x in data:
                if isinstance(x, six.string_types):
                    setattr(self, x.upper(), StringTree(x, None, self))
                elif isinstance(x, dict):
                    for k, v in x.items():
                        setattr(self, k.upper(), StringTree(k, v, self))

    def __getattr__(self, name):
        return getattr(str(self), name)

    def __iter__(self):
        return iter(self.fqn)

    def __eq__(self, other):
        return str(self) == other

    def __repr__(self):
        return self.fqn

    def __hash__(self):
        return hash(self.fqn)

    @property
    def fqn(self):
        parent = self.parent
        family = deque()
        while parent:
            family.appendleft(parent.name)
            parent = parent.parent
        family.append(self.name)
        return '.'.join(family)


memoize_cache = {}

def memoize(func):
    """
    Implements function return value caching based on argument signature.
    See: https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    """
    cache = func.cache = memoize_cache[func] = {}

    @six.wraps(func)
    def memoizer(*args, **kwargs):
        key = ''.join((str(args), str(kwargs)))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return memoizer


def subtopic(*args):
    return '.'.join([str(x) for x in args])


def get_engine():
    """ Stub """
    return 'nats'


def get_concrete_engine(engine_name):
    module = importlib.import_module('.'.join(['taxi.core.engines', engine_name]))
    return module.ConcreteEngine


def list_modules(package_name):
    paths = importlib.import_module(package_name).__path__
    # Use a set because some may be both source and compiled.
    submods = set()
    for pathname in paths:
        for filename in os.listdir(pathname):
            for suffix, _, _ in imp.get_suffixes():
                if filename.endswith(suffix) and not filename.startswith('__init__'):
                    submods.add(filename.partition(suffix)[0])

    return submods
