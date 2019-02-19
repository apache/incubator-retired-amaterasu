import unittest

from pyspark.sql import DataFrame

from amaterasu_pyspark.runtime import ama_context
from amaterasu.datastores import DatasetNotFoundError, DatasetTypeNotSupported


class DatastoresTests(unittest.TestCase):

    def test_loading_an_existing_generic_dataset_should_not_be_implemented(self):
        self.assertRaises(NotImplementedError,  ama_context.get_dataset, "mydataset")

    def test_loading_an_unsupported_dataset_should_raise_an_exception(self):
        self.assertRaises(DatasetTypeNotSupported, ama_context.get_dataset, "unsupported")

    def test_loading_a_dataset_that_is_not_defined_should_raise_an_exception(self):
        self.assertRaises(DatasetNotFoundError, ama_context.get_dataset, "notfound")

    def test_load_dataset_from_file_should_return_a_dataframe(self):
        df = ama_context.get_dataset('input_file')
        self.assertEquals(type(df), DataFrame)