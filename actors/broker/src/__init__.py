# Declare Flask app for brokers
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os

# Declare Flask app for brokers
app = Flask(__name__)

# Declare db using SQL Alchemy, give the db address
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://postgres:admin@localhost:5432/{os.getenv('DATABASE_NAME')}"
db = SQLAlchemy(app)

from db_models import *

with app.app_context():
    if os.getenv("DEBUG") == "true":
        db.drop_all()
        db.create_all()
    else:
        db.create_all()


# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views


