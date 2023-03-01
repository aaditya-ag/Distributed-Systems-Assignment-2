from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import threading
import requests
from time import sleep

app = Flask(__name__)

app.config[
    "SQLALCHEMY_DATABASE_URI"
] = "postgresql://postgres:admin@localhost:5432/distributed_queue"
db = SQLAlchemy(app)

from db_models import *

master_queue = None # MasterQueue() # instantiate master queue

def health_checker():
    while True:
        try:
            response = requests.get("http://127.0.0.1:8000/")
            if response.status_code != 200:
                print("Not Responding...")
            else:
                print("Alive")
        except Exception as e:
            print("Not Responding...")
            # print(f"ERROR: {str(e)}")
            pass

        sleep(2)


with app.app_context():
    db.create_all()

    print("Starting health check threaad")
    health_check_daemon = threading.Thread(
        target=health_checker,
        args=(),
        daemon=True,
    )

    health_check_daemon.start()
