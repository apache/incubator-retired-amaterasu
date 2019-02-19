import yaml
import enum
import abc
from typing import Dict, Any, Type


class DatasetTypes(enum.Enum):
    Hive = 'hive'
    File = 'file'
    Generic = 'generic'


class DatasetNotFoundError(Exception):
    def __init__(self, msg: str):
        super(DatasetNotFoundError, self).__init__(msg)


class DatasetTypeNotSupported(Exception):
    pass

class BaseDatastore(abc.ABC):

    def __init__(self, dataset_conf: Dict):
        self.dataset_conf = dataset_conf

    @abc.abstractmethod
    def load_dataset(self) -> Any:
        pass

    @abc.abstractmethod
    def persist_dataset(self, dataset: Any, overwrite: bool):
        pass


class GenericDatastore(BaseDatastore):

    def __init__(self, dataset_conf, *args):
        super(GenericDatastore, self).__init__(dataset_conf)

    def load_dataset(self):
        raise NotImplementedError("Loading generic datasets is not supported")

    def persist_dataset(self, dataset, overwrite):
        raise NotImplementedError("Persisting generic datasets is not supported")


class BaseDatasetManager(abc.ABC):

    _registered_datastores: Dict[str, Type[BaseDatastore]] = {}

    def __init__(self):
        self._registered_datastores[DatasetTypes.Generic.value] = GenericDatastore
        with open('datasets.yml', 'r', encoding='utf-8') as f:
            self._datasets_conf = yaml.load(f)

    def _find_dataset_config(self, dataset_name: str) -> Dict:
        for dataset_type, dataset_configurations in self._datasets_conf.items():
            for config in dataset_configurations:
                if config['name'] == dataset_name:
                    dataset_config = config.copy()
                    dataset_config['type'] = dataset_type
                    return dataset_config
        else:
            raise DatasetNotFoundError("No dataset by name \"{}\" defined".format(dataset_name))

    @abc.abstractmethod
    def get_datastore(self, datastore_cls: Type[BaseDatastore], dataset_conf: Dict) -> BaseDatastore:
        """
        Get the concrete datastore required to handle this type of dataset.
        The reason this is an abstract method is to provide different runtime types the ability to inject their
        own logic and dependencies for handling their specific datastores.
        e.g. - A spark related datastore would need the SparkSession object available.
        :param dataset_conf: The dataset configuration loaded from the datasets yml
        :param datastore_cls: A concrete datastore class
        :return:
        """
        pass

    def _get_datastore_cls(self, dataset_conf: Dict) -> Type[BaseDatastore]:
        try:
            datastore_cls = self._registered_datastores[dataset_conf['type']]
            return datastore_cls
        except KeyError:
            raise DatasetTypeNotSupported("Unsupported dataset type: {}".format(dataset_conf['type']))

    def load_dataset(self, dataset_name: str) -> Any:
        dataset_conf = self._find_dataset_config(dataset_name)
        datastore_cls = self._get_datastore_cls(dataset_conf)
        datastore = self.get_datastore(datastore_cls, dataset_conf)
        return datastore.load_dataset()

    def persist_dataset(self, dataset_name: str, dataset: Any, overwrite: bool):
        dataset_conf = self._find_dataset_config(dataset_name)
        datastore_cls = self._get_datastore_cls(dataset_conf)
        datastore = self.get_datastore(datastore_cls, dataset_conf)
        datastore.persist_dataset(dataset, overwrite)

    def get_dataset_configuration(self, dataset_name: str) -> Dict:
        return self._find_dataset_config(dataset_name)
