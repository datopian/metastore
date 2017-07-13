import time
import unittest
from importlib import import_module
from elasticsearch import Elasticsearch, NotFoundError

LOCAL_ELASTICSEARCH = 'localhost:9200'

from os_package_registry import PackageRegistry

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
        self.pr = PackageRegistry(es_instance=self.es)

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

    def indexSomeRecordsToTestMapping(self):
        for i in range(3):
            self.pr.save_model('package-id-%d' % i,
                               '', {
                                   'author': 'BlaBla%d@test2.com' % i,
                                   'title': 'This dataset is number test%d' % i
                               }, {}, {},
                               'BlaBla%d@test2.com' % (i+1), '', True)

    def indexSomeRealLookingRecords(self, amount):
        for i in range(amount):
            self.pr.save_model('package-id-%d' % i,
                               '', {
                                   'author': 'The one and only author number%d' % (i+1),
                                   'title': 'This dataset is number%d' % i
                               }, {}, {},
                               'The one and only author number%d' % (i+1), '', True)

    def indexSomePrivateRecords(self):
        i = 0
        for owner in ['owner1', 'owner2']:
            for private in [True, False]:
                for loaded in [True, False]:
                    for content in ['cat', 'dog']:
                        self.pr.save_model('%s-%s-%s-%s' % (owner, private, loaded, content),
                                           '', {
                                               'author': 'The one and only author number%d' % (i+1),
                                               'title': 'This dataset is number%d, content is %s' % (i, content),
                                               'owner': owner,
                                               'private': private
                                           }, {}, {},
                                           'The one and only author number%d' % (i+1), '', loaded)
                        i += 1
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

    def test___search___q_param_no_recs_no_results(self):
        self.indexSomeRealLookingRecords(0)
        self.assertEquals(len(module.search('package', None, {'q': ['"author"']})), 0)

    def test___search___q_param_some_recs_no_results(self):
        self.indexSomeRealLookingRecords(2)
        self.assertEquals(len(module.search('package', None, {'q': ['"writer"']})), 0)

    def test___search___q_param_some_recs_some_results(self):
        self.indexSomeRealLookingRecords(2)
        results = module.search('package', None, {'q': ['"number1"']})
        self.assertEquals(len(results), 1)

    def test___search___q_param_some_recs_all_results(self):
        self.indexSomeRealLookingRecords(10)
        results = module.search('package', None, {'q': ['"dataset shataset"']})
        self.assertEquals(len(results), 10)

    def test___search___empty_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('package', None)
        self.assertEquals(len(recs), 4)
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'owner1-False-True-cat',
                                  'owner2-False-True-cat',
                                  'owner1-False-True-dog',
                                  'owner2-False-True-dog',
                                  })

    def test___search___empty_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('package', 'owner1')
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'owner1-False-False-cat',
                                  'owner1-False-True-cat',
                                  'owner1-True-False-cat',
                                  'owner1-True-True-cat',
                                  'owner2-False-True-cat',
                                  'owner1-False-False-dog',
                                  'owner1-False-True-dog',
                                  'owner1-True-False-dog',
                                  'owner1-True-True-dog',
                                  'owner2-False-True-dog',
                                  })
        self.assertEquals(len(recs), 10)

    def test___search___q_param_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('package', None, {'q': ['"cat"']})
        self.assertEquals(len(recs), 2)
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'owner1-False-True-cat',
                                  'owner2-False-True-cat',
                                  })

    def test___search___q_param_anonymous_search_with_param(self):
        self.indexSomePrivateRecords()
        recs = module.search('package', None, {'q': ['"cat"'], 'package.owner': ['"owner1"']})
        self.assertEquals(len(recs), 1)
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'owner1-False-True-cat'})

    def test___search___q_param_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs = module.search('package', 'owner1', {'q': ['"cat"']})
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'owner1-False-False-cat',
                                  'owner1-False-True-cat',
                                  'owner1-True-False-cat',
                                  'owner1-True-True-cat',
                                  'owner2-False-True-cat',
                                  })
        self.assertEquals(len(recs), 5)

    def test___search___q_param_with_similar_param(self):
        self.indexSomeRecordsToTestMapping()
        recs = module.search('package', None, {'q': ['"test2"']})
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs = module.search('package', None, {'q': ['"dataset"'], 'package.author': ['"BlaBla2@test2.com"']})
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs = module.search('package', None, {'package.author': ['"BlaBla2@test2.com"']})
        ids = set([r['id'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)
