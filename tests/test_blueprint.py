import unittest
try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch
from importlib import import_module
module = import_module('metastore.blueprint')


class createTest(unittest.TestCase):

    # Actions

    def setUp(self):
        self.addCleanup(patch.stopall)
        self.controllers = patch.object(module, 'controllers').start()

    # Tests

    def test(self):
        self.assertTrue(module.create())
