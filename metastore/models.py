import os
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

_engine = None

ENABLED_SEARCHES = {
    'package': {
        'index': 'packages',
        'doc_type': 'package',
        '_source': ['id', 'model', 'package',
                    'origin_url', 'loaded', 'last_update'],
        'owner': 'package.owner',
        'private': 'package.private',
        'q_fields': ['package.title',
                     'package.author',
                     'package.owner',
                     'package.description',
                     'package.regionCode',
                     'package.countryCode',
                     'package.cityCode'],
    }
}


def _get_engine():
    global _engine
    if _engine is None:
        es_host = os.environ['DATAHUB_ELASTICSEARCH_ADDRESS']
        _engine = Elasticsearch(hosts=[es_host], use_ssl='https' in es_host)

    return _engine


def build_dsl(kind_params, userid, kw):
    dsl = {'bool': {'should': [], 'must': [], 'minimum_should_match': 1}}
    # All Datasets:
    all_datasets = {
        'bool': {
            'should': [{'match': {kind_params['private']: False}},
                       {'filtered':
                        {'filter': {'missing': {'field':
                                                kind_params['private']}}}},
                       ],
            'must_not': {'match': {'loaded': False}},
            'minimum_should_match': 1
        }
    }
    dsl['bool']['should'].append(all_datasets)

    # User datasets
    if userid is not None:
        user_datasets = \
            {'bool': {'must': {'match': {kind_params['owner']: userid}}}}
        dsl['bool']['should'].append(user_datasets)

    # Query parameters
    q = kw.get('q')
    if q is not None:
        dsl['bool']['must'].append({
                'multi_match': {
                    'query': json.loads(q[0]),
                    'fields': kind_params['q_fields']
                }
            })
    for k, v_arr in kw.items():
        if k.split('.')[0] in kind_params['_source']:
            dsl['bool']['must'].append({
                    'bool': {
                        'should': [{'match': {k: json.loads(v)}}
                                   for v in v_arr],
                        'minimum_should_match': 1
                    }
               })

    if len(dsl['bool']['must']) == 0:
        del dsl['bool']['must']
    if len(dsl['bool']) == 0:
        del dsl['bool']
    if len(dsl) == 0:
        dsl = {}
    else:
        dsl = {'query': dsl, 'explain': True}
    # logger.info('Sending DSL %s', json.dumps(dsl))
    return dsl


def query(kind, userid, size=100, **kw):
    kind_params = ENABLED_SEARCHES.get(kind)
    if kind_params is None:
        return None
    try:
        # Arguments received from a network request come in kw, as a mapping
        # between param_name and a list of received values.
        # If size was provided by the user, it will be a list, so we take its
        # first item.
        if type(size) is list:
            size = size[0]
        api_params = dict([
            ('index', kind_params['index']),
            ('doc_type', kind_params['doc_type']),
            ('size', int(size)),
            ('_source', kind_params['_source'])
        ])

        body = build_dsl(kind_params, userid, kw)
        api_params['body'] = json.dumps(body)
        ret = _get_engine().search(**api_params)
        if ret.get('hits') is not None:
            return [hit['_source'] for hit in ret['hits']['hits']]
    except (NotFoundError, json.decoder.JSONDecodeError, ValueError) as e:
        logging.error("query: %r" % e)
        pass
    return None
