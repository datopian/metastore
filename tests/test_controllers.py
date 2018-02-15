import datetime
import unittest
from importlib import import_module
from elasticsearch import Elasticsearch, NotFoundError

LOCAL_ELASTICSEARCH = 'localhost:9200'

module = import_module('metastore.controllers')

class SearchTest(unittest.TestCase):

    # Actions
    MAPPING = {
        'id': {"type": "string", "analyzer": "keyword"},
        'name': {"type": "string", "analyzer": "keyword"},
        'title': {"type": "string", "analyzer": "simple"},
        'description': {"type": "string", "analyzer": "standard"},
        'datahub': {
            'type': 'object',
            'properties': {
                'owner': {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "ownerid": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "findability": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "flowid": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "stats": {
                    "type": "object",
                    "properties": {
                        "rowcount": {
                            "type": "integer",
                            "index": "not_analyzed"
                        },
                        "bytes": {
                            "type": "integer",
                            "index": "not_analyzed"
                        }
                    }
                }
            }
        },
        'datapackage': {
            'type': 'object',
            'properties': {
                'readme': {
                    "type": "string",
                    "analyzer": "standard",
                }
            }
        }
    }

    words = [
        'headphones', 'ideal', 'naive', 'city', 'flirtation',
        'annihilate', 'crypt', 'ditch', 'glacier', 'megacity'
    ]

    def setUp(self):

        # Clean index
        self.es = Elasticsearch(hosts=[LOCAL_ELASTICSEARCH])
        try:
            self.es.indices.delete(index='datahub')
            self.es.indices.delete(index='events')
        except NotFoundError:
            pass
        self.es.indices.create('datahub')
        mapping = {'dataset': {'properties': self.MAPPING}}
        self.es.indices.put_mapping(doc_type='dataset',
                                    index='datahub',
                                    body=mapping)

        self.es.indices.create('events')
        mapping = {'event': {'properties': {'timestamp': {'type': 'date'}}}}
        self.es.indices.put_mapping(doc_type='event',
                                    index='events',
                                    body=mapping)

    def search(self, kind, *args, **kwargs):
        ret = module.search(kind, *args, **kwargs)
        self.assertLessEqual(len(ret['results']), ret['summary']['total'])
        return ret['results'], ret['summary']

    def indexSomeEventRecords(self, amount):
        for i in range(amount):
            body = dict(
                timestamp=datetime.datetime(2000+i, 1, 1, 0, 0, 0),
                event_entity='flow' if i % 3 else 'login',
                event_action='finished' if i % 4 else 'deleted',
                owner='datahub',
                ownerid='datahubid',
                dataset='dataset' + str(i),
                status='OK',
                messsage='',
                findability='published' if i % 2 else 'unlisted',
                payload={'flow-id': 'datahub/dataset'}
            )
            self.es.index('events', 'event', body)
        self.es.indices.flush('events')

    def indexSomeRecords(self, amount):
        self.es.indices.delete(index='datahub')
        for i in range(amount):
            body = {
                'name': True,
                'title': i,
                'license': 'str%s' % i,
                'datahub': {
                    'name': 'innername',
                    'findability': 'published',
                    'stats': {
                        'bytes': 10
                    }
                }
            }
            self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexSomeRecordsToTestMapping(self):

        for i in range(3):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number test %s' % self.words[i],
                'datahub': {
                    'owner': 'BlaBla%d@test2.com' % i,
                    'findability': 'published',
                    'stats': {
                        'bytes': 10
                    }
                },
            }
            self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexSomeRealLookingRecords(self, amount):
        for i in range(amount):
            body = {
                'name': 'package-id-%d' % i,
                'title': 'This dataset is number %s' % self.words[i%10],
                'datahub': {
                    'owner': 'The one and only owner number %s' % (self.words[(i+1)%10]),
                    'findability': 'published',
                    'stats': {
                        'bytes': 10
                    }
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
                            'findability': private,
                            'stats': {
                                'bytes': 10
                            }
                        }
                    }
                    i += 1
                    self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexSomePrivateRecordsWithReadme(self):
        i = 0
        for owner in ['owner1', 'owner2']:
            for private in ['published', 'else']:
                for content in ['cat', 'dog']:
                    body = {
                        'name': '%s-%s-%s' % (owner, private, content),
                        'title': 'This dataset is number%d, content is %s' % (i, content),
                        'datahub': {
                            'owner': 'The one and only owner number%d' % (i + 1),
                            'ownerid': owner,
                            'findability': private,
                            'stats': {
                                'bytes': 10
                            }
                        },
                        'datapackage': {
                            'readme':'some readme text '+str(i)+' which should be searched through '
                        }
                    }
                    i += 1
                    self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    def indexMultipleUserRecords(self):
        for owner in ['core', 'anonymous', 'friend', 'other']:
            for findability in ['published', 'unlisted', 'private']:

                body = {
                    'name': '%s-dataset' % owner,
                    'title': 'This dataset is owned by %s' % owner,
                    'datahub': {
                        'owner': 'Example',
                        'ownerid': owner,
                        'findability': findability,
                        'stats': {
                            'bytes': 10
                        }
                    },
                    'datapackage': {
                        'readme':'some readme text which should be searched through '
                    }
                }
                self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')

    # Tests Datahub
    def test___search___all_values_and_empty(self):
        self.assertEquals(self.search('dataset', None), ([], {'total': 0, 'totalBytes': 0.0}))

    def test___search___all_values_and_one_result(self):
        self.indexSomeRecords(1)
        res, summary = self.search('dataset', None)
        self.assertEquals(len(res), 1)
        self.assertEquals(summary['total'], 1)
        self.assertEquals(summary['totalBytes'], 10)

    def test___search___all_values_and_two_results(self):
        self.indexSomeRecords(2)
        res, summary = self.search('dataset', None)
        self.assertEquals(len(res), 2)
        self.assertEquals(summary['total'], 2)
        self.assertEquals(summary['totalBytes'], 20)

    def test___search___filter_simple_property(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'license': ['"str7"']})
        self.assertEquals(len(res), 1)
        self.assertEquals(summary['total'], 1)
        self.assertEquals(summary['totalBytes'], 10)

    def test___search___filter_numeric_property(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'title': ["7"]})
        self.assertEquals(len(res), 1)
        self.assertEquals(summary['total'], 1)
        self.assertEquals(summary['totalBytes'], 10)

    def test___search___filter_boolean_property(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'name': ["true"]})
        self.assertEquals(len(res), 10)
        self.assertEquals(summary['total'], 10)
        self.assertEquals(summary['totalBytes'], 100)

    def test___search___filter_multiple_properties(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'license': ['"str6"'], 'title': ["6"]})
        self.assertEquals(len(res), 1)
        self.assertEquals(summary['total'], 1)
        self.assertEquals(summary['totalBytes'], 10)

    def test___search___filter_multiple_values_for_property(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'license': ['"str6"','"str7"']})
        self.assertEquals(len(res), 2)
        self.assertEquals(summary['total'], 2)
        self.assertEquals(summary['totalBytes'], 20)

    def test___search___filter_inner_property(self):
        self.indexSomeRecords(7)
        res, summary = self.search('dataset', None, {"datahub.name": ['"innername"']})
        self.assertEquals(len(res), 7)
        self.assertEquals(summary['total'], 7)
        self.assertEquals(summary['totalBytes'], 70)

    def test___search___filter_no_results(self):
        res, summary = self.search('dataset', None, {'license': ['"str6"'], 'title': ["7"]})
        self.assertEquals(len(res), 0)
        self.assertEquals(summary['total'], 0)
        self.assertEquals(summary['totalBytes'], 0)

    def test___search___filter_bad_value(self):
        ret = module.search('dataset', None, {'license': ['str6'], 'title': ["6"]})
        self.assertEquals(ret['results'], [])
        self.assertEquals(ret['summary']['total'], 0)
        self.assertEquals(ret['summary']['totalBytes'], 0)
        self.assertIsNotNone(ret['error'])

    def test___search___filter_nonexistent_property(self):
        ret = module.search('dataset', None, {'license': ['str6'], 'boxing': ["6"]})
        self.assertEquals(ret['results'], [])
        self.assertEquals(ret['summary']['total'], 0)
        self.assertEquals(ret['summary']['totalBytes'], 0)
        self.assertIsNotNone(ret['error'])

    def test___search___returns_limited_size(self):
        self.indexSomeRecords(10)
        res, summary = self.search('dataset', None, {'size':['4']})
        self.assertEquals(len(res), 4)
        self.assertEquals(summary['total'], 10)
        self.assertEquals(summary['totalBytes'], 100)

    def test___search___not_allows_more_than_50(self):
        self.indexSomeRecords(105)
        res, summary = self.search('dataset', None, {'size':['105']})
        self.assertEquals(len(res), 100)
        self.assertEquals(summary['total'], 105)
        self.assertEquals(summary['totalBytes'], 1050)

    def test___search___returns_results_from_given_index(self):
        self.indexSomeRecords(5)
        res, summary = self.search('dataset', None, {'from':['3']})
        self.assertEquals(len(res), 2)
        self.assertEquals(summary['total'], 5)
        self.assertEquals(summary['totalBytes'], 50)

    def test___search___q_param_no_recs_no_results(self):
        self.indexSomeRealLookingRecords(0)
        res, summary = self.search('dataset', None, {'q': ['"owner"']})
        self.assertEquals(len(res), 0)
        self.assertEquals(summary['total'], 0)
        self.assertEquals(summary['totalBytes'], 0)

    def test___search___q_param_some_recs_no_results(self):
        self.indexSomeRealLookingRecords(2)
        res, summary = self.search('dataset', None, {'q': ['"writer"']})
        self.assertEquals(len(res), 0)
        self.assertEquals(summary['total'], 0)
        self.assertEquals(summary['totalBytes'], 0)

    def test___search___q_param_some_recs_some_results(self):
        self.indexSomeRealLookingRecords(2)
        res, summary = self.search('dataset', None, {'q': ['"ideal"']})
        self.assertEquals(len(res), 1)
        self.assertEquals(summary['total'], 1)
        self.assertEquals(summary['totalBytes'], 10)

    def test___search___q_param_some_recs_all_results(self):
        self.indexSomeRealLookingRecords(10)
        res, summary = self.search('dataset', None, {'q': ['"dataset shataset"']})
        self.assertEquals(len(res), 10)
        self.assertEquals(summary['total'], 10)
        self.assertEquals(summary['totalBytes'], 100)

    def test___search___empty_anonymous_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('dataset', None)
        self.assertEquals(len(recs), 4)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  'owner1-published-dog',
                                  'owner2-published-dog',
                                  })

    def test___search___empty_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('dataset', 'owner1')
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
        recs, _ = self.search('dataset', None, {'q': ['"cat"']})
        self.assertEquals(len(recs), 2)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner2-published-cat',
                                  })

    def test___search___q_param_anonymous_search_with_param(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('dataset', None, {'q': ['"cat"'], 'datahub.ownerid': ['"owner1"']})
        self.assertEquals(len(recs), 1)
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat'})

    def test___search___q_param_authenticated_search(self):
        self.indexSomePrivateRecords()
        recs, _ = self.search('dataset', 'owner1', {'q': ['"cat"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'owner1-published-cat',
                                  'owner1-else-cat',
                                  'owner2-published-cat',
                                  })
        self.assertEquals(len(recs), 3)

    def test___search___q_param_with_similar_param(self):
        self.indexSomeRecordsToTestMapping()
        recs, _ = self.search('dataset', None, {'q': ['"naive"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs, _ = self.search('dataset', None, {'q': ['"dataset"'], 'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

        recs, _ = self.search('dataset', None, {'datahub.owner': ['"BlaBla2@test2.com"']})
        ids = set([r['name'] for r in recs])
        self.assertSetEqual(ids, {'package-id-2'})
        self.assertEquals(len(recs), 1)

    def test_search__q_param_in_readme(self):
        body = {
            'name': True,
            'title': 'testing',
            'license': 'str',
            'datahub': {
                'name': 'innername',
                'findability': 'published',
                'stats': {
                    'bytes': 10
                }
            },
            'datapackage': {
                'readme': 'text only in README',
                'not_readme': 'NOTREADME'
            },
        }
        self.es.index('datahub', 'dataset', body)
        self.es.indices.flush('datahub')
        recs, _ = self.search('dataset', None, {'q': ['"README"']})
        self.assertEquals(len(recs), 1)
        ## Make sure not queries unlisted fields
        recs, _ = self.search('dataset', None, {'q': ['"NOTREADME"']})
        self.assertEquals(len(recs), 0)

    def test__search__q_param_in_readme_with_more_records(self):
        self.indexSomePrivateRecordsWithReadme()
        recs, _ = self.search('dataset', None, {'q': ['"readme"']})
        self.assertEquals(len(recs), 4)
        ## Make sure not queries unlisted fields
        recs, _ = self.search('dataset', None, {'q': ['"NOTREADME"']})
        self.assertEquals(len(recs), 0)

    def test__search__q_core_gets_prefered(self):
        self.indexMultipleUserRecords()
        recs, _ = self.search('dataset', None, {'q': ['"readme"']})
        self.assertEquals(len(recs), 4)
        self.assertEquals(recs[0]['name'], 'core-dataset')

    # Tests Events
    def test___search___all_events_are_empty(self):
        self.assertEquals(self.search('events', None), ([], {'total': 0, 'totalBytes': 0.0}))

    def test___search___all_event_are_there_but_unlisted(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', None)
        self.assertEquals(len(res), 5)

    def test___search___all_event_are_there_with_id_including_unlisted(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid')
        self.assertEquals(len(res), 10)

    def test___search___all_event_filter_with_findability(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid', {'findability': ['"unlisted"']})
        self.assertEquals(len(res), 5)

    def test___search___all_event_filter_with_action(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid', {'event_action': ['"finished"']})
        self.assertEquals(len(res), 7)

    def test___search___all_event_filter_with_entity(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid', {'event_entity': ['"flow"']})
        self.assertEquals(len(res), 6)

    def test___search___all_event_filter_with_entity_and_action(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid', {
            'event_entity': ['"flow"'],
            'event_action': ['"finished"']
        })
        self.assertEquals(len(res), 4)

    def test___search___all_event_sorts_with_timestamp(self):
        self.indexSomeEventRecords(10)
        res, _ = self.search('events', 'datahubid')
        self.assertEquals(res[0]['timestamp'], '2009-01-01T00:00:00')
        self.assertEquals(res[9]['timestamp'], '2000-01-01T00:00:00')
        res, _ = self.search('events', 'datahubid', {'sort': ['"asc"']})
        self.assertEquals(res[0]['timestamp'], '2000-01-01T00:00:00')
        self.assertEquals(res[9]['timestamp'], '2009-01-01T00:00:00')
