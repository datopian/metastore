import time
import unittest
from importlib import import_module
from elasticsearch import Elasticsearch, NotFoundError

LOCAL_ELASTICSEARCH = 'localhost:9200'

module = import_module('metastore.controllers')


class SearchTest(unittest.TestCase):

    # Actions
    MAPPING = {
        'datahub': {
            'type': 'object',
            'properties': {
                'owner': {
                    "type": "string",
                    "index": "not_analyzed",
                }
            }
        }
    }

    def setUp(self):

        # Clean index
        self.es = Elasticsearch(hosts=[LOCAL_ELASTICSEARCH])
        try:
            self.es.indices.delete(index='datasets')
        except NotFoundError:
            pass
        self.es.indices.create('datasets')
        mapping = {'dataset': {'properties': self.MAPPING}}
        self.es.indices.put_mapping(doc_type='dataset',
                                    index='datasets',
                                    body=mapping)

    def indexSomeRecords(self, amount):
        self.es.indices.delete(index='datasets')
        for i in range(amount):
            body = {
                'name': True,
                'title': i,
                'license': 'str%s' % i,
                'datahub': {
                    'name': 'innername'
                }
            }
            self.es.index('datasets', 'dataset', body)
        self.es.indices.flush('datasets')

    def indexSomeRecordsToTestMapping(self):
        for i in range(3):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number test%d' % i,
                'datahub': {
                    'owner': 'BlaBla%d@test2.com' % i,
                },
            }
            self.es.index('datasets', 'dataset', body)
        self.es.indices.flush('datasets')

    def indexSomeRealLookingRecords(self, amount):
        for i in range(amount):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number%d' % i,
                'datahub': {
                    'owner': 'The one and only owner number%d' % (i+1),
                },
                'loaded': True
            }
            self.es.index('datasets', 'dataset', body)
        self.es.indices.flush('datasets')

    def indexSomePrivateRecords(self):
        i = 0
        for owner in ['owner1', 'owner2']:
            for private in ['published', 'else']:
                for content in ['cat', 'dog']:
                    body = {
                        'name': '%s-%s-%s' % (owner, private, content),
                        'title': 'This dataset is number%d, content is %s' % (i, content),
                        'datahub': {
                            'owner': 'The one and only owner number%d' % (i+1),
                            'ownerid': owner,
                            'findability': private
                        }
                    }
                    i += 1
                    self.es.index('datasets', 'dataset', body)
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
        self.assertEquals(len(module.search(None, {'license': ['"str7"']})), 1)

    def test___search___filter_numeric_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'title': ["7"]})), 1)

    def test___search___filter_boolean_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'name': ["true"]})), 10)

    def test___search___filter_multiple_properties(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'license': ['"str6"'], 'title': ["6"]})), 1)

    def test___search___filter_multiple_values_for_property(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'license': ['"str6"','"str7"']})), 2)

    def test___search___filter_inner_property(self):
        self.indexSomeRecords(7)
        self.assertEquals(len(module.search(None, {"datahub.name": ['"innername"']})), 7)

    def test___search___filter_no_results(self):
        self.assertEquals(len(module.search(None, {'license': ['"str6"'], 'title': ["7"]})), 0)

    def test___search___filter_bad_value(self):
        self.assertEquals(module.search(None, {'license': ['str6'], 'title': ["6"]}), None)

    def test___search___filter_nonexistent_property(self):
        self.assertEquals(module.search(None, {'license': ['str6'], 'boxing': ["6"]}), None)

    def test___search___returns_limited_size(self):
        self.indexSomeRecords(10)
        self.assertEquals(len(module.search(None, {'size':['4']})), 4)

    def test___search___not_allows_more_than_50(self):
        self.indexSomeRecords(55)
        self.assertEquals(len(module.search(None, {'size':['55']})), 50)

    def test___search___returns_results_from_given_index(self):
        self.indexSomeRecords(5)
        self.assertEquals(len(module.search(None, {'from':['3']})), 2)

    def test___search___q_param_no_recs_no_results(self):
        self.indexSomeRealLookingRecords(0)
        self.assertEquals(len(module.search(None, {'q': ['"owner"']})), 0)

    def test___search___q_param_some_recs_no_results(self):
        self.indexSomeRealLookingRecords(2)
        self.assertEquals(len(module.search(None, {'q': ['"writer"']})), 0)

    def test___search___q_param_some_recs_some_results(self):
        self.indexSomeRealLookingRecords(2)
        results = module.search(None, {'q': ['"number1"']})
        self.assertEquals(len(results), 1)

    def test___search___q_param_some_recs_all_results(self):
        self.indexSomeRealLookingRecords(10)
        results = module.search(None, {'q': ['"dataset shataset"']})
        self.assertEquals(len(results), 10)

    def test___search___empty_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs = module.search(None)
        self.assertEquals(len(recs), 4)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  'owner1-published-dog',
                                  'owner2-published-dog',
                                  })

    def test___search___empty_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('owner1')
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner1-else-cat',
                                  'owner2-published-cat',
                                  'owner1-published-dog',
                                  'owner1-else-dog',
                                  'owner2-published-dog',
                                  })
        self.assertEquals(len(recs), 6)

    def test___search___q_param_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs = module.search(None, {'q': ['"cat"']})
        self.assertEquals(len(recs), 2)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  })

    def test___search___q_param_anonymous_search_with_param(self):
        self.indexSomePrivateRecords()
        recs = module.search(None, {'q': ['"cat"'], 'datahub.ownerid': ['"owner1"']})
        self.assertEquals(len(recs), 1)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat'})

    def test___search___q_param_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('owner1', {'q': ['"cat"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner1-else-cat',
                                  'owner2-published-cat',
                                  })
        self.assertEquals(len(recs), 3)

    def test___search___q_param_with_similar_param(self):
        self.indexSomeRecordsToTestMapping()
        recs = module.search(None, {'q': ['"test2"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs = module.search(None, {'q': ['"dataset"'], 'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs = module.search(None, {'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)
