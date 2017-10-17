import elasticsearch

from .models import query


def search(kind, userid, args={}):
    """Initiate an elasticsearch query
    """
    try:
        res = query(kind, userid, **args)
        return res
    except elasticsearch.exceptions.ElasticsearchException as e:
        return {
            'total': 0,
            'results': [],
            'error': str(e)
        }
