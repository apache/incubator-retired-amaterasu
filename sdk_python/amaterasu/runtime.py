"""
Base classes for SDK
"""
import logging
import stomp
import yaml
import os
import atexit
import abc
from munch import Munch, munchify


class ImproperlyConfiguredError(Exception):
    pass


class AmaActiveMQNotificationHandler(logging.Handler):

    def create_mq(self):
        self.mq = stomp.Connection()
        self.mq.start()
        self.mq.connect()

    def __init__(self, level=logging.NOTSET):
        formatter = logging.Formatter('%(message)s')
        self.setFormatter(formatter)
        self.create_mq()
        self.queue_name = os.getenv('LEADER_JMS_QUEUE')
        if not self.queue_name:
            raise ImproperlyConfiguredError("No JMS queue name was supplied by the leader")
        super().__init__(level)

    def emit(self, record):
        self.mq.send(body=record, destination=self.queue_name)


class BaseAmaContext(abc.ABC):

    instance = None
    is_initialized = False

    @abc.abstractmethod
    def persist(self, action_name, dataset_name, dataset):
        pass

    @abc.abstractmethod
    def get_dataset(self, action_name, dataset_name):
        pass

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = _LazyProxy(cls, *args, **kwargs)
        return cls.instance


class _LazyProxy:

    instance = None

    def __init__(self, cls, *args, **kwargs):
        """
        Utility singleton object that is really instantiated only
        when it is first accessed. We use it to instantiate our contexts to
        provide an easier API for users.

        e.g. for the Python SDK developer:
        Let's say that you have a class "AAAContext" for the framework named
        "AAA", it is the SDK developer's responsibility to wrap it with
        a LazyProxy object.
        An example usage woul be:
        >>> ama_context = _LazyProxy(AAAContext, *args, **kwargs)
        At this point, the AAAContext isn't instantiated.
        When the framework user tries to access the context for the first time,
        only then the AAAContext is instantiated.
        e.g. -
        >>> ama_context.get_dataset("somename", "somevalue") <-- instance is now created.
        >>> ama_context.get_dataset("anothername", "anothervalue") <-- instance is reused

        :param cls:
        :param args:
        :param kwargs:
        """
        super(_LazyProxy, self).__setattr__('cls', cls)
        super(_LazyProxy, self).__setattr__('args', args)
        super(_LazyProxy, self).__setattr__('kwargs', kwargs)

    def _get_or_create_instance(self):
        instance = super(_LazyProxy, self).__getattribute__('instance')
        if not instance:
            cls = super(_LazyProxy, self).__getattribute__('cls')
            args = super(_LazyProxy, self).__getattribute__('args')
            kwargs = super(_LazyProxy, self).__getattribute__('kwargs')
            instance = object.__new__(cls)
            instance.__init__(*args, **kwargs)
            super(_LazyProxy, self).__setattr__('instance', instance)
        return instance

    def __getattr__(self, item):
        instance = super(_LazyProxy, self).__getattribute__('_get_or_create_instance')()
        return getattr(instance, item)

    def __setattr__(self, key, value):
        instance = self._get_or_create_instance()
        return setattr(instance, key, value)


class Environment(Munch):
    pass


class Notifier(logging.Logger):

    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        handler = _LazyProxy(AmaActiveMQNotificationHandler)
        self.addHandler(handler)


def _create_configuration():
    _dict = {
        'job_metadata': None,
        'env': None,
        'exports': None
    }
    with open('env.yml', 'r') as f:
        _dict['env'] = yaml.load(f.read())
    with open('datastores.yml', 'r') as f:
        _dict['exports'] = yaml.load(f.read())
    with open('runtime.yml', 'r') as f:
        _dict['job_metadata'] = yaml.load(f.read())

    return munchify(_dict, factory=Environment)


conf = _create_configuration()
logging.setLoggerClass(Notifier)
notifier = logging.getLogger(__name__)
atexit.register(lambda: notifier.info('Action {} finished successfully'.format(conf.job_metadata.actionName)))
__all__ = ['BaseAmaContext', 'conf', 'notifier']
