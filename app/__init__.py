from flask import Flask
from .urls import main

def create_app():
    app = Flask(__name__)
    app.register_blueprint(main)

    return app
