# Declare Flask app for brokers
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from src.models import URLBook

urlbook = URLBook()

# Declare Flask app for brokers
app = Flask(__name__)




# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views


