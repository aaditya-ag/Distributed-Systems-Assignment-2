from flask_restful import Resource, Api, reqparse
from src import (
    app,
    db,
    master_queue
)

from db_models import *

api = Api(app)

class TopicAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()
        if master_queue.has_topic(args["topic_name"]):
            return {
                "status": "Failure",
                "message": "Topic already exists"
            }, 400
        
        master_queue.add_topic(args["topic_name"])
        return {
            "status": "Success"
        }, 200
    

class ProducerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()

        if not master_queue.has_topic(args["topic_name"]):
            master_queue.add_topic(args["topic_name"])
        
        producer_id = master_queue.add_producer(args["topic_name"])
        return {
            "status": "Success",
            "producer_id": producer_id
        }, 200
    
class ConsumerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()

        consumer_id = master_queue.add_consumer(args["topic_name"])
        return {
            "status": "Success",
            "consumer_id": consumer_id
        }, 200
    
