import elasticsearch

from .models import query


def search(kind, userid, args={}):
    """Initiate an elasticsearch query
    """
    try:
        hits = query(kind, userid, **args)
        return hits
    except elasticsearch.exceptions.ElasticsearchException:
        return []
