from flask import Flask
from flask_cors import CORS
from .blueprint import create as search

def create():
    """Create application.
    """

    # Create application
    app = Flask('service', static_folder=None)
    app.config['DEBUG'] = True

    # CORS support
    CORS(app, supports_credentials=True)
    app.register_blueprint(search(), url_prefix='/metastore/')

    # Return application
    return app
