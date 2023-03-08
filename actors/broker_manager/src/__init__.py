from flask import Flask
from flask.json.provider import DefaultJSONProvider
from flask_sqlalchemy import SQLAlchemy
import requests
from threading import Thread
from time import sleep
import os

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
db = SQLAlchemy(app)


from db_models import *
from src.models import MasterQueue

master_queue = MasterQueue()

# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views

def health_checker():
    while True:
        brokers = master_queue.get_brokers()
        for broker in brokers:
            try:
                broker_url = broker.get_base_url()
                response = requests.get(broker_url)
                assert(response.status_code == 200)
                print(f"{broker} Alive")
                if not broker.is_alive():
                    with app.app_context():
                        broker.update_running_status(True)
            except Exception as e:
                print(f"{broker} Not Responding...")
                if broker.is_alive():
                    with app.app_context():
                        broker.update_running_status(False)

        sleep(2)


with app.app_context():
    if os.environ.get("DEBUG") == "true":
        db.drop_all()
        db.create_all()
    else:
        db.create_all()

    # fetch if it is a read_only manager
    master_queue.do_init_sync()
    
    print("Done")

    # finally reflect the changes done in database into in-memory data structures
    master_queue.fetch_from_db() 
    
    print("Starting health check thread")
    health_check_daemon = Thread(
        target=health_checker,
        daemon=True,
    )
    health_check_daemon.start()