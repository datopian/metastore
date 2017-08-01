import time
import unittest
from importlib import import_module
from elasticsearch import Elasticsearch, NotFoundError

LOCAL_ELASTICSEARCH = 'localhost:9200'

module = import_module('metastore.controllers')


class SearchTest(unittest.TestCase):

    # Actions

    def setUp(self):

        # Clean index
        self.es = Elasticsearch(hosts=[LOCAL_ELASTICSEARCH])
        try:
            self.es.indices.delete(index='packages')
        except NotFoundError:
            pass
        self.es.indices.create('packages')

    def indexSomeRecords(self, amount):
        self.es.indices.delete(index='packages')
        for i in range(amount):
            body = {
                'id': True,
                'package': i,
                'model': 'str%s' % i,
                'origin_url': {
                    'name': 'innername'
                }
            }
            self.es.index('packages', 'package', body)
        self.es.indices.flush('packages')


    # Tests
    def test___search___all_values_and_empty(self):
        self.assertEquals(len(module.search('package', None)), 0)

    def test___search___all_values_and_one_result(self):
        self.indexSomeRecords(1)
        self.assertEquals(len(module.search('package', None)), 1)

    def test___search___all_values_and_two_results(self):
        self.indexSomeRecords(2)
        self.assertEquals(len(module.search('package', None)), 2)

    def test___search___filter_simple_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search('package', None, {'model': ['"str7"']})), 1)

    def test___search___filter_numeric_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search('package', None, {'package': ["7"]})), 1)

    def test___search___filter_boolean_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search('package', None, {'id': ["true"]})), 10)

    def test___search___filter_multiple_properties(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search('package', None, {'model': ['"str6"'], 'package': ["6"]})), 1)

    def test___search___filter_multiple_values_for_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search('package', None, {'model': ['"str6"','"str7"']})), 2)

    def test___search___filter_inner_property(self):
        self.indexSomeRecords(7)
        self.assertEquals(len(module.search('package', None, {"origin_url.name": ['"innername"']})), 7)

    def test___search___filter_no_results(self):
        self.assertEquals(len(module.search('package', None, {'model': ['"str6"'], 'package': ["7"]})), 0)

    def test___search___filter_bad_value(self):
        self.assertEquals(module.search('package', None, {'model': ['str6'], 'package': ["6"]}), None)

    def test___search___filter_nonexistent_kind(self):
        self.assertEquals(module.search('box', None, {'model': ['str6'], 'package': ["6"]}), None)

    def test___search___filter_nonexistent_property(self):
        self.assertEquals(module.search('box', None, {'model': ['str6'], 'boxing': ["6"]}), None)
