import elasticsearch

from .models import query


def search(userid, args={}):
    """Initiate an elasticsearch query
    """
    try:
        res = query(userid, **args)
        return res
    except elasticsearch.exceptions.ElasticsearchException as e:
        return {
            'total': 0,
            'results': [],
            'error': str(e)
        }
