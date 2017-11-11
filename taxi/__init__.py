from taxi.common import config
from taxi.factory import NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_concrete_engine


ConcreteEngine = get_concrete_engine(config['engine'])


def Node(*namespaces):
    return NodeFactory(ConcreteEngine, *namespaces)


def Manager(*namespaces):
    return ManagerFactory(ConcreteEngine, *namespaces)


def Worker(*namespaces):
    return WorkerFactory(ConcreteEngine, *namespaces)
