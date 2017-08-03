import elasticsearch

from .models import query


def search(userid, args={}):
    """Initiate an elasticsearch query
    """
    try:
        hits = query(userid, **args)
        return hits
    except elasticsearch.exceptions.ElasticsearchException:
        return []
