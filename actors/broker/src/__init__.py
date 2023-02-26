# Declare Flask app for brokers
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# from flask import request, jsonify, make_response

# Declare Flask app for brokers
app = Flask(__name__)

# Declare db using SQL Alchemy


# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views


