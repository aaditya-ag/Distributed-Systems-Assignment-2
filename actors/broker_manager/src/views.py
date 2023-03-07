from flask_restful import Resource, Api, reqparse
from datetime import datetime
from src import (
    app,
    master_queue
)


from db_models import *

from src.http_status_codes import *
from src.utils import sync_db

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
            }, HTTP_400_BAD_REQUEST
        
        master_queue.add_topic(args["topic_name"])
        return {
            "status": "Success"
        }, HTTP_201_CREATED
    

class ProducerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()
        
        producer_id, topic_locations = master_queue.add_producer(args["topic_name"])
        return {
            "status": "Success",
            "producer_id": producer_id,
            "topic_locations": topic_locations,
        }, HTTP_201_CREATED
    
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
            }, HTTP_400_BAD_REQUEST
        else:
            return {
                "status": "Success",
                "consumer_id": consumer_id
            }, HTTP_201_CREATED
    
class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("consumer_id", type=int, required=True, help="Consumer id required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()
        
        message = master_queue.dequeue(args["topic_name"], args["consumer_id"])
        if message is None:
            return {
                "status": "Failure",
                "message": "Unable to fetch message due to no more messages or broker is down"
            }, HTTP_400_BAD_REQUEST
        else:
            return {
                "status": "Success",
                "message": message
            }, HTTP_200_OK     

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("producer_id", type=int, required=True, help="Producer id required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("message", required=True, help="Message required")
        parser.add_argument("partition_id", type=int)
        args = parser.parse_args()
        
        if master_queue.enqueue(args["topic_name"], args["producer_id"], args["message"], args.get("partition_id")):
            return {
                "status": "Success"
            }, HTTP_201_CREATED
        else:
            return {
                "status": "Failure",
                "message": "Unable to add message due to invalid producer id or broker is down"
            }, HTTP_400_BAD_REQUEST  

class MessageSizeAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("consumer_id", type=int, required=True, help="Consumer id required")
        args = parser.parse_args()
        
        message_size = master_queue.count_unread_messages(args["topic_name"], args["consumer_id"])
        if message_size is None:
            return {
                "status": "Failure",
                "message": "Unable to fetch message size due to invalid input"
            }, HTTP_400_BAD_REQUEST
        else:
            return {
                "status": "Success",
                "message_size": message_size
            }, HTTP_200_OK

class BrokerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("broker_ip", required=True, help="Broker ip required")
        parser.add_argument("broker_port", required=True, help="Broker port required")
        args = parser.parse_args()
        
        master_queue.add_broker(args["broker_ip"], args["broker_port"])
        return {
            "status": "Success"
        }, HTTP_201_CREATED


class LiveSyncAPI(Resource):
    """
    A single DB entry update
    """
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("operation", required=True, help="Operation required")
        parser.add_argument("table_name", required=True, help="Table Name required")
        parser.add_argument("data", required=True, help="Table Data required")
        parser.add_argument("checkpoint", required=True, help="checkpoint required")
        args = parser.parse_args()
        sync_db.sync(operation=args["operation"], table=args["table"], data=args["data"])

        if args["checkpoint"]:
            master_queue.create_checkpoint()

        return {
            "status": "Success"
        }, HTTP_201_CREATED

class InitSyncAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument(
            "timestamp", 
            required=True, 
            type=lambda x: datetime.fromisoformat(x), 
            help="ISO TimeStamp Required")
        args = parser.parse_args()

        timestamp = args["timestamp"]
        print(f"Init Sync Requested for timestamp {timestamp}")
        updates = []
        for topic in TopicModel.query.filter(TopicModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["Topic", topic.as_dict()])
        
        for broker in BrokerModel.query.filter(BrokerModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["Broker", broker.as_dict()])

        for producer in ProducerModel.query.filter(ProducerModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["Producer", producer.as_dict()])

        for consumer in ConsumerModel.query.filter(ConsumerModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["Consumer", consumer.as_dict()])

        for tpb_entry in TPBMapModel.query.filter(TPBMapModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["TPBMap", tpb_entry.as_dict()])

        for tpl_entry in TPLMapModel.query.filter(TPLMapModel.updated_at > timestamp).order_by('updated_at').all():
            updates.append(["TPLMap", tpl_entry.as_dict()])

        # Send in ascending order for having the updates in db addition order
        updates.sort(key=lambda x: x[1]["updated_at"])
        print(updates)

        return {
            "status": "Success",
            "updates": updates
        }, HTTP_200_OK

api.add_resource(TopicAPI, "/topics")
api.add_resource(ProducerAPI, "/producers")
api.add_resource(ConsumerAPI, "/consumers")
api.add_resource(MessageAPI, "/messages")
api.add_resource(MessageSizeAPI, "/unread_messages")
api.add_resource(BrokerAPI, "/brokers")
api.add_resource(LiveSyncAPI, "/live_sync")
api.add_resource(InitSyncAPI, "/init_sync")