# Declare Flask app for brokers
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# Declare Flask app for brokers
app = Flask(__name__)

# Declare db using SQL Alchemy, give the db address
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:admin@localhost:5432/distributed_queue"
db = SQLAlchemy(app)

from db_models import *

with app.app_context():
    db.create_all()


# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views


