from flask import Flask
from .routes import main

# 👇 Import ingest function
from .ingest import load_or_reuse

def create_app():
    # Run data ingestion only if needed
    load_or_reuse(force=False)

    app = Flask(__name__)
    app.register_blueprint(main)

    return app
