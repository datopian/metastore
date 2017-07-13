from flask import Flask
from flask_cors import CORS
from flask_session import Session
from .blueprint import create as search

def create():
    """Create application.
    """

    # Create application
    app = Flask('service', static_folder=None)
    app.config['DEBUG'] = True

    # CORS support
    CORS(app, supports_credentials=True)

    # Session
    sess = Session()
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['SECRET_KEY'] = 'DataHub rocks'
    sess.init_app(app)

    app.register_blueprint(search(), url_prefix='/package/')

    # Return application
    return app
