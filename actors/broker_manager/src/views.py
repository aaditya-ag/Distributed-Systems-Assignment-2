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
        if consumer_id == None:
            return {
                "status": "Failure",
                "message": "The topic does not exist"
            }, 400
        else:
            return {
                "status": "Success",
                "consumer_id": consumer_id
            }, 200
    
class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("consumer_id", required=True, help="Consumer id required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()
        
        message = master_queue.get_message(args["topic_name"], args["consumer_id"])
        if message is None:
            return {
                "status": "Failure",
                "message": "Unable to fetch message due to no more messages or broker is down"
            }, 400
        else:
            return {
                "status": "Success",
                "message": message
            }, 200     

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("producer_id", required=True, help="Producer id required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("message", required=True, help="Message required")
        parser.add_argument("partition_id")
        args = parser.parse_args()
        
        if master_queue.add_message(args["topic_name"], args["producer_id"], args["message"], args.get("partition_id")):
            return {
                "status": "Success"
            }, 200
        else:
            return {
                "status": "Failure",
                "message": "Unable to add message due to invalid producer id or broker is down"
            }, 400  

class MessageSizeAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("consumer_id", required=True, help="Consumer id required")
        args = parser.parse_args()
        
        message_size = master_queue.find_size(args["topic_name"], args["consumer_id"])
        if message_size is None:
            return {
                "status": "Failure",
                "message": "Unable to fetch message size due to invalid input"
            }, 400
        else:
            return {
                "status": "Success",
                "message_size": message_size
            }, 200

class BrokerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("broker_ip", required=True, help="Broker ip required")
        parser.add_argument("broker_port", required=True, help="Broker port required")
        args = parser.parse_args()
        
        master_queue.add_broker(args["broker_ip"], args["broker_port"])
        return {
            "status": "Success"
        }, 200
