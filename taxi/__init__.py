from taxi.common import config
from taxi.mixins import SensorMixin
from taxi.factory import NodeFactory, ManagerFactory, WorkerFactory
from taxi.util import get_concrete_engine, subtopic


ConcreteEngine = get_concrete_engine(config['engine'])


def Node(*namespaces):
    return NodeFactory(ConcreteEngine, *namespaces)


def Manager(*namespaces):
    return ManagerFactory(ConcreteEngine, *namespaces)


def Worker(*namespaces):
    return WorkerFactory(ConcreteEngine, *namespaces)


def Sensor(*namespaces):

    class Sensor(ConcreteEngine, SensorMixin):
        NAMESPACE = subtopic('sensor', *namespaces)

    return Sensor
