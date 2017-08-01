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
            self.es.indices.delete(index='datasets')
        except NotFoundError:
            pass
        self.es.indices.create('datasets')

    def indexSomeRecords(self, amount):
        self.es.indices.delete(index='datasets')
        for i in range(amount):
            body = {
                'id': True,
                'dataset': i,
                'model': 'str%s' % i,
                'origin_url': {
                    'name': 'innername'
                }
            }
            self.es.index('datasets','dataset', body)
        self.es.indices.flush('datasets')


    # Tests
    def test___search___all_values_and_empty(self):
        self.assertEquals(len(module.search(None)), 0)

    def test___search___all_values_and_one_result(self):
        self.indexSomeRecords(1)
        self.assertEquals(len(module.search(None)), 1)

    def test___search___all_values_and_two_results(self):
        self.indexSomeRecords(2)
        self.assertEquals(len(module.search(None)), 2)

    def test___search___filter_simple_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'model': ['"str7"']})), 1)

    def test___search___filter_numeric_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'dataset': ["7"]})), 1)

    def test___search___filter_boolean_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'id': ["true"]})), 10)

    def test___search___filter_multiple_properties(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'model': ['"str6"'], 'dataset': ["6"]})), 1)

    def test___search___filter_multiple_values_for_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'model': ['"str6"','"str7"']})), 2)

    def test___search___filter_inner_property(self):
        self.indexSomeRecords(7)
        self.assertEquals(len(module.search(None, {"origin_url.name": ['"innername"']})), 7)

    def test___search___filter_no_results(self):
        self.assertEquals(len(module.search(None, {'model': ['"str6"'], 'dataset': ["7"]})), 0)

    def test___search___filter_bad_value(self):
        self.assertEquals(module.search(None, {'model': ['str6'], 'dataset': ["6"]}), None)
