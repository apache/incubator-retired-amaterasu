"""
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
import logging
import stomp
import yaml
import os
import abc
from munch import Munch, munchify
from typing import Any
from amaterasu.base import BaseDatasetManager

logger = logging.root
formatter = logging.Formatter()
handler = logging.StreamHandler()
handler.formatter = formatter
logger.addHandler(handler)


def _get_absolute_file_path(file_name):
    if not os.path.isabs(file_name):
        cwd = os.getcwd()
        return os.path.join(cwd, file_name)
    else:
        return file_name


class ImproperlyConfiguredError(Exception):
    pass


class Environment(Munch):
    pass


class RuntimeNotSupportedError(Exception):
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


class BaseAmaContextBuilder(abc.ABC):

    def __init__(self):
        self.env_conf_path = _get_absolute_file_path('env.yaml')
        self.runtime_conf_path = _get_absolute_file_path('runtime.yaml')
        self.datasets_conf_path = _get_absolute_file_path('datasets.yaml')
        self.ama_conf = self._create_env()
        self._frameworks = self._resolve_supported_frameworks()

    def _create_env(self):
        try:
            _dict = {
                'runtime': {},
                'env': {},
                'datasets': {}
            }
            with open(self.env_conf_path, 'r') as f:
                _dict['env'] = yaml.load(f.read())
            with open(self.runtime_conf_path, 'r') as f:
                _dict['runtime'] = yaml.load(f.read())
            with open(self.datasets_conf_path, 'r') as f:
                _dict['datasets'] = yaml.load(f.read())
            return munchify(_dict)
        except FileNotFoundError:
            logger.exception("Could not load env data!")
            return None

    def _resolve_supported_frameworks(self):
        supported_frameworks = {}
        for subclass in self.__class__.__subclasses__():
            if hasattr(subclass, '_framework_name'):
                supported_frameworks[subclass._framework_name] = subclass
        return supported_frameworks

    def set_env_path(self, env_path):
        self.env_conf_path = _get_absolute_file_path(env_path)
        self.ama_conf = self._create_env()
        return self

    def set_runtime_path(self, runtime_path):
        self.runtime_conf_path = _get_absolute_file_path(runtime_path)
        self.ama_conf = self._create_env()
        return self

    def set_datasets_path(self, datasets_path):
        self.datasets_conf_path = _get_absolute_file_path(datasets_path)
        self.ama_conf = self._create_env()
        return self

    def as_type(self, framework_name):
        try:
            framework_builder = self._frameworks[framework_name]()
            framework_builder.set_env_path(self.env_conf_path)
            framework_builder.set_datasets_path(self.datasets_conf_path)
            framework_builder.set_runtime_path(self.runtime_conf_path)
            return framework_builder
        except KeyError:
            raise RuntimeNotSupportedError(
                "Runtime for '{}' is not supported, are you sure it is installed?".format(framework_name))

    @abc.abstractmethod
    def build(self):
        pass


class BaseAmaContext(abc.ABC):

    def __init__(self, ama_conf: Munch):
        self._ama_conf = ama_conf

    @classmethod
    @abc.abstractmethod
    def builder(cls):
        pass

    @property
    def env(self):
        return self._ama_conf['env']

class LoaderAmaContext(BaseAmaContext, abc.ABC):
    @property
    @abc.abstractmethod
    def dataset_manager(self) -> BaseDatasetManager:
        pass

    def persist(self, dataset_name: str, dataset: Any, overwrite: bool = False):
        self.dataset_manager.persist_dataset(dataset_name, dataset, overwrite)

    def get_dataset(self, dataset_name: str):
        return self.dataset_manager.load_dataset(dataset_name)



class Notifier(logging.Logger):

    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.addHandler(AmaActiveMQNotificationHandler)


def _create_configuration():
    _dict = {
        'job_metadata': None,
        'env': None
    }
    with open('env.yml', 'r') as f:
        _dict['env'] = yaml.load(f.read())
    with open('runtime.yml', 'r') as f:
        _dict['job_metadata'] = yaml.load(f.read())

    return munchify(_dict, factory=Environment)


# logging.setLoggerClass(Notifier)
# notifier = logging.getLogger(__name__)
__all__ = ['BaseAmaContext', 'BaseAmaContextBuilder', 'LoaderAmaContext']
