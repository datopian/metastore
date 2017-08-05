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
            self.es.indices.delete(index='datahub')
        except NotFoundError:
            pass
        self.es.indices.create('datahub')
        mapping = {'dataset': {'properties': self.MAPPING}}
        self.es.indices.put_mapping(doc_type='dataset',
                                    index='datahub',
                                    body=mapping)

    def search(self, *args, **kwargs):
        ret = module.search(*args, **kwargs)
        self.assertLessEqual(len(ret['results']), ret['total'])
        return ret['results'], ret['total']

    def indexSomeRecords(self, amount):
        self.es.indices.delete(index='datahub')
        for i in range(amount):
            body = {
                'name': True,
                'title': i,
                'license': 'str%s' % i,
                'datahub': {
                    'name': 'innername',
                    'findability': 'published'
                }
            }
            self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexSomeRecordsToTestMapping(self):
        for i in range(3):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number test%d' % i,
                'datahub': {
                    'owner': 'BlaBla%d@test2.com' % i,
                    'findability': 'published'
                },
            }
            self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexSomeRealLookingRecords(self, amount):
        for i in range(amount):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number%d' % i,
                'datahub': {
                    'owner': 'The one and only owner number%d' % (i+1),
                    'findability': 'published'
                },
                'loaded': True
            }
            self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

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
                    self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    # Tests
    def test___search___all_values_and_empty(self):
        self.assertEquals(self.search(None), ([], 0))

    def test___search___all_values_and_one_result(self):
        self.indexSomeRecords(1)
        res, ttl = self.search(None)
        self.assertEquals(len(res), 1)
        self.assertEquals(ttl, 1)

    def test___search___all_values_and_two_results(self):
        self.indexSomeRecords(2)
        res, ttl = self.search(None)
        self.assertEquals(len(res), 2)
        self.assertEquals(ttl, 2)

    def test___search___filter_simple_property(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'license': ['"str7"']})
        self.assertEquals(len(res), 1)
        self.assertEquals(ttl, 1)

    def test___search___filter_numeric_property(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'title': ["7"]})
        self.assertEquals(len(res), 1)
        self.assertEquals(ttl, 1)

    def test___search___filter_boolean_property(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'name': ["true"]})
        self.assertEquals(len(res), 10)
        self.assertEquals(ttl, 10)

    def test___search___filter_multiple_properties(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'license': ['"str6"'], 'title': ["6"]})
        self.assertEquals(len(res), 1)
        self.assertEquals(ttl, 1)

    def test___search___filter_multiple_values_for_property(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'license': ['"str6"','"str7"']})
        self.assertEquals(len(res), 2)
        self.assertEquals(ttl, 2)

    def test___search___filter_inner_property(self):
        self.indexSomeRecords(7)
        res, ttl = self.search(None, {"datahub.name": ['"innername"']})
        self.assertEquals(len(res), 7)
        self.assertEquals(ttl, 7)

    def test___search___filter_no_results(self):
        res, ttl = self.search(None, {'license': ['"str6"'], 'title': ["7"]})
        self.assertEquals(len(res), 0)
        self.assertEquals(ttl, 0)

    def test___search___filter_bad_value(self):
        ret = module.search(None, {'license': ['str6'], 'title': ["6"]})
        self.assertEquals(ret['results'], [])
        self.assertEquals(ret['total'], 0)
        self.assertIsNotNone(ret['error'])

    def test___search___filter_nonexistent_property(self):
        ret = module.search(None, {'license': ['str6'], 'boxing': ["6"]})
        self.assertEquals(ret['results'], [])
        self.assertEquals(ret['total'], 0)
        self.assertIsNotNone(ret['error'])

    def test___search___returns_limited_size(self):
        self.indexSomeRecords(10)
        res, ttl = self.search(None, {'size':['4']})
        self.assertEquals(len(res), 4)
        self.assertEquals(ttl, 10)

    def test___search___not_allows_more_than_50(self):
        self.indexSomeRecords(55)
        res, ttl = self.search(None, {'size':['55']})
        self.assertEquals(len(res), 50)
        self.assertEquals(ttl, 55)

    def test___search___returns_results_from_given_index(self):
        self.indexSomeRecords(5)
        res, ttl = self.search(None, {'from':['3']})
        self.assertEquals(len(res), 2)
        self.assertEquals(ttl, 5)

    def test___search___q_param_no_recs_no_results(self):
        self.indexSomeRealLookingRecords(0)
        res, ttl = self.search(None, {'q': ['"owner"']})
        self.assertEquals(len(res), 0)
        self.assertEquals(ttl, 0)

    def test___search___q_param_some_recs_no_results(self):
        self.indexSomeRealLookingRecords(2)
        res, ttl = self.search(None, {'q': ['"writer"']})
        self.assertEquals(len(res), 0)
        self.assertEquals(ttl, 0)

    def test___search___q_param_some_recs_some_results(self):
        self.indexSomeRealLookingRecords(2)
        res, ttl = self.search(None, {'q': ['"number1"']})
        self.assertEquals(len(res), 1)
        self.assertEquals(ttl, 1)

    def test___search___q_param_some_recs_all_results(self):
        self.indexSomeRealLookingRecords(10)
        res, ttl = self.search(None, {'q': ['"dataset shataset"']})
        self.assertEquals(len(res), 10)
        self.assertEquals(ttl, 10)

    def test___search___empty_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search(None)
        self.assertEquals(len(recs), 4)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  'owner1-published-dog',
                                  'owner2-published-dog',
                                  })

    def test___search___empty_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('owner1')
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
        recs, _ = self.search(None, {'q': ['"cat"']})
        self.assertEquals(len(recs), 2)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  })

    def test___search___q_param_anonymous_search_with_param(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search(None, {'q': ['"cat"'], 'datahub.ownerid': ['"owner1"']})
        self.assertEquals(len(recs), 1)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat'})

    def test___search___q_param_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('owner1', {'q': ['"cat"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner1-else-cat',
                                  'owner2-published-cat',
                                  })
        self.assertEquals(len(recs), 3)

    def test___search___q_param_with_similar_param(self):
        self.indexSomeRecordsToTestMapping()
        recs, _ = self.search(None, {'q': ['"test2"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs, _ = self.search(None, {'q': ['"dataset"'], 'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs, _ = self.search(None, {'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)
