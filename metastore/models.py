import os
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


logging.root.setLevel(logging.INFO)
logging.getLogger('elasticsearch').setLevel(logging.DEBUG)

_engine = None

ENABLED_SEARCHES = {
    'dataset': {
        'index': 'datahub',
        'doc_type': 'dataset',
        'owner': 'datahub.ownerid',
        'findability': 'datahub.findability',
        'q_fields': [
            'title',
            'datahub.owner',
            'datahub.ownerid',
            'datapackage.readme',
        ],
    },
    'events': {
        'index': 'events',
        'doc_type': 'event',
        'owner': 'ownerid',
        'findability': 'findability',
        'timestamp': 'timestamp',
        'q_fields': []
    }
}

BOOSTS = {
    'title': '^5',
    'datahub.owner': '',
    'datahub.ownerid': '',
    'datapackage.readme': '^2',
}


def _get_engine():
    global _engine
    if _engine is None:
        es_host = os.environ['DATAHUB_ELASTICSEARCH_ADDRESS']
        _engine = Elasticsearch(hosts=[es_host], use_ssl='https' in es_host)

    return _engine


def build_dsl(kind_params, userid, kw):
    dsl = {'bool': {
        'should': [],
        'must': [], 'minimum_should_match': 1}}
    # All Datasets:
    all_datasets = {
        'bool': {
            'should': [{'match': {kind_params['findability']: 'published'}}],
            'minimum_should_match': 1
        }
    }
    boost_core = {
        'bool': {
            'should': [{ "match": { "datahub.ownerid": {"query": "core", "boost": 4.5}}}],
            'must': [{'match': {kind_params['findability']: 'published'}}],
            'minimum_should_match': 1
        }
    }
    dsl['bool']['should'].append(all_datasets)
    dsl['bool']['should'].append(boost_core)

    # User datasets
    if userid is not None:
        user_datasets = \
            {'bool': {'must': {'match': {kind_params['owner']: userid}}}}
        dsl['bool']['should'].append(user_datasets)

    # Allow sorting event results
    sort_by = kw.pop('sort', ['desc'])[0].replace('"', '')
    sort = []
    if kind_params.get('timestamp'):
        sort.append({'timestamp': {'order' : sort_by}})

    # Query parameters (for not to mess with other parameters we should pop)
    q = kw.pop('q', None)
    if q is not None:
        dsl['bool']['must'].append({
                'multi_match': {
                    'query': json.loads(q[0]),
                    'fields': [f+(BOOSTS.get(f, '')) for f in kind_params['q_fields']],
                    'type': 'most_fields'
                }
            })
    for k, v_arr in kw.items():
        dsl['bool']['must'].append({
                'bool': {
                    'should': [{'term': {k: json.loads(v)}}
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
        dsl = {'query': dsl, 'explain': True, 'sort': sort}

    aggs = { 'total_bytes': { 'sum': { 'field': 'datahub.stats.bytes' } } }
    dsl['aggs'] = aggs

    return dsl


def query(kind, userid, size=50, **kw):
    kind_params = ENABLED_SEARCHES.get(kind)
    try:
        # Arguments received from a network request come in kw, as a mapping
        # between param_name and a list of received values.
        # If size was provided by the user, it will be a list, so we take its
        # first item.
        if type(size) is list:
            size = size[0]
            if int(size) > 100:
                size = 100

        from_ = int(kw.pop('from', [0])[0])

        api_params = dict([
            ('index', kind_params['index']),
            ('doc_type', kind_params['doc_type']),
            ('size', size),
            ('from_', from_),
            ('search_type', 'dfs_query_then_fetch')
        ])

        body = build_dsl(kind_params, userid, kw)
        api_params['body'] = json.dumps(body)
        ret = _get_engine().search(**api_params)
        logging.info('Performing query %r', kind_params)
        logging.info('api_params %r', api_params)
        logging.info('ret %r', ret)
        if ret.get('hits') is not None:
            results = [hit['_source'] for hit in ret['hits']['hits']]
            total = ret['hits']['total']
            total_bytes = ret.get('aggregations')['total_bytes']['value']
        else:
            results = []
            total = 0
            total_bytes = 0
        return {
            'results': results,
            'summary': {
                "total": total,
                "totalBytes": total_bytes
            }
        }
    except (NotFoundError, json.decoder.JSONDecodeError, ValueError) as e:
        logging.error("query: %r" % e)
        return {
            'results': [],
            'summary': {
                "total": 0,
                "totalBytes": 0
            },
            'error': str(e)
        }
