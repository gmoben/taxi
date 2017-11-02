from taxi.mixins import (
    ClientMixin,
    NodeMixin,
    ManagerMixin,
    WorkerMixin)
from taxi.util import (
    memoize,
    subtopic
)

@memoize
def ClientFactory(engine_class):
    """Build a Client using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteClient class

    """
    class ConcreteClient(engine_class, ClientMixin):
        pass

    return ConcreteClient


@memoize
def NodeFactory(engine_class, *namespaces):
    """Build a Node using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteNode class

    """
    class ConcreteNode(engine_class, NodeMixin):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteNode


@memoize
def ManagerFactory(engine_class, *namespaces):
    """Build a Manager using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteManager class

    """
    class ConcreteManager(engine_class, ManagerMixin):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteManager


@memoize
def WorkerFactory(engine_class, *namespaces):
    """Build a Worker using a concrete Engine.

    :param ConcreteEngine engine_class: AbstractEngine subclass
    :returns: ConcreteWorker class

    """
    class ConcreteWorker(engine_class, WorkerMixin):
        NAMESPACE = subtopic(*namespaces)

    return ConcreteWorker
