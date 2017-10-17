import os
import jwt
from flask import Blueprint, abort, request
from flask_jsonpify import jsonpify

from . import controllers

PRIVATE_KEY = os.environ.get('PRIVATE_KEY')


def create():
    """Create blueprint.
    """

    # Create instance
    blueprint = Blueprint('search', 'search')

    # Controller Proxies
    search_controller = controllers.search

    def search(kind='dataset'):
        token = request.headers.get('auth-token') or request.values.get('jwt')
        userid = None
        try:
            if token is not None:
                token = jwt.decode(token, PRIVATE_KEY)
                userid = token.get('userid')
        except jwt.InvalidTokenError:
            pass
        ret = search_controller(kind, userid, request.args)
        if ret is None:
            abort(400)
        return jsonpify(ret)

    # Register routes
    blueprint.add_url_rule(
        'search', 'search', search, methods=['GET'])
    blueprint.add_url_rule(
        'search/<kind>', 'events', search, methods=['GET'])

    # Return blueprint
    return blueprint
