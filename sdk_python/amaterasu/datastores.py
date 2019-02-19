from abc import ABC
from typing import Any

import yaml
import enum
import abc


class DatasetTypes(enum.Enum):
    Hive = 'hive'
    File = 'file'
    Generic = 'generic'


class DatasetNotFoundError(Exception):
    def __init__(self, msg):
        super(DatasetNotFoundError, self).__init__(msg)


class _BaseDatastore(abc.ABC):

    def __init__(self, dataset_conf):
        self.dataset_conf = dataset_conf

    @abc.abstractmethod
    def load_dataset(self):
        pass

    @abc.abstractmethod
    def persist_dataset(self, dataset):
        pass


class _GenericDatastore(_BaseDatastore):

    def __init__(self, dataset_conf):
        super(_GenericDatastore, self).__init__(dataset_conf)

    def load_dataset(self):
        raise NotImplementedError("Loading generic datasets is not supported")

    def persist_dataset(self, dataset):
        raise NotImplementedError("Persisting generic datasets is not supported")


class DatasetMeta(type):

    def __init__(cls, cls_name, cls_bases, cls_attrs) -> None:
        if cls_bases:
            cls._registered_datastores[DatasetTypes.Generic] = _GenericDatastore
        super().__init__(cls_name, cls_bases, cls_attrs)


class DatasetManager(metaclass=DatasetMeta):

    _registered_datastores = {}

    def __init__(self):
        with open('datasets.yml', 'r', encoding='utf-8') as f:
            self._datasets_conf = yaml.load(f)

    def _find_dataset_config(self, dataset_name):
        for dataset_type, dataset_configurations in self._datasets_conf.items():
            for config in dataset_configurations:
                if config['name'] == dataset_name:
                    dataset_config = config.copy()
                    dataset_config['type'] = dataset_type
                    return dataset_config
        else:
            raise DatasetNotFoundError("No dataset by name \"{}\" defined".format(dataset_name))

    def _get_datastore(self, dataset_conf):
        try:
            datastore_cls = self._registered_datastores[dataset_conf['type']]
            datastore = datastore_cls(dataset_conf)
            return datastore
        except KeyError:
            raise NotImplementedError("Unsupported dataset type: {}".format(dataset_conf['type']))

    def load_dataset(self, dataset_name):
        dataset_conf = self._find_dataset_config(dataset_name)
        datastore = self._get_datastore(dataset_conf)
        return datastore.load_dataset()

    def persist_dataset(self, dataset_name, dataset):
        dataset_conf = self._find_dataset_config(dataset_name)
        datastore = self._get_datastore(dataset_conf)
        datastore.persist_dataset(dataset)

    def get_dataset_configuration(self, dataset_name):
        return self._find_dataset_config(dataset_name)