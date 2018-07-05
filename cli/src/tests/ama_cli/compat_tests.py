import unittest
from amaterasu.cli import compat

class TestCompatRunSubprocess(unittest.TestCase):

    def test_run_subprocess_with_valid_input_should_execute_subprocess_successfully(self):
        inpt = ['echo', 'HELLO']
        compat.run_subprocess(*inpt)