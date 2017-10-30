import importlib

from taxi.core.base import NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_engine, get_concrete_engine


ConcreteEngine = get_concrete_engine(get_engine())


def Node(node_type, *namespaces):
    return NodeFactory(ConcreteEngine, *namespaces)


def Manager(*namespaces):
    return ManagerFactory(ConcreteEngine, *namespaces)


def Worker(*namespaces):
    return WorkerFactory(ConcreteEngine, *namespaces)