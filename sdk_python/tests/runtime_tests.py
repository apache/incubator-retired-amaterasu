import unittest
from amaterasu.runtime import BaseAmaContext
from amaterasu.datastores import DatasetNotFoundError


class DatastoresTests(unittest.TestCase):

    def setUp(self):
        self.ama_context = BaseAmaContext()

    def test_loading_an_existing_generic_dataset_should_not_be_implemented(self):
        self.assertRaises(NotImplementedError,  self.ama_context.get_dataset, "mydataset")

    def test_loading_an_unsupported_dataset_should_raise_an_exception(self):
        self.assertRaises(NotImplementedError, self.ama_context.get_dataset, "unsupported")

    def test_loading_a_dataset_that_is_not_defined_should_raise_an_exception(self):
        self.assertRaises(DatasetNotFoundError, self.ama_context.get_dataset, "notfound")
