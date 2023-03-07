from flask_restful import Resource, Api, reqparse

from src import (
    app,
    urlbook
)

import requests

from src.http_status_codes import *

api = Api(app)

class TopicAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()

        
        wr_only_mgr_url = urlbook.get_wr_url()
        
        try:
            response = requests.post(
                url = wr_only_mgr_url + "/topics",
                json={
                    "topic_name": args["topic_name"]
                }
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        if response.status_code != HTTP_201_CREATED:
            return {
                "status": "Failure",
                "message": "Topic already exists"
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success"
        }, HTTP_201_CREATED


class ProducerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()

        wr_only_mgr_url = urlbook.get_wr_url()

        try:
            response = requests.post(
                url = wr_only_mgr_url + "/producers",
                json={
                    "topic_name": args["topic_name"]
                }
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        return {
            "status": "Success",
            "producer_id": response.json().get("producer_id"),
            "topic_locations": response.json().get("topic_locations")
        }, HTTP_201_CREATED

class ConsumerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        args = parser.parse_args()

        wr_only_mgr_url = urlbook.get_wr_url()

        try:
            response = requests.post(
                url = wr_only_mgr_url + "/consumers",
                json={
                    "topic_name": args["topic_name"]
                }
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        if response.status_code != HTTP_201_CREATED:
            return {
                "status": "Failure",
                "message": "Topic doesn't exists"
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success",
            "consumer_id": response.json().get("producer_id")
        }, HTTP_201_CREATED
    
class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("consumer_id", type=int, required=True, help="Consumer id required")
        args = parser.parse_args()

        rd_only_mgr_url = urlbook.get_random_live_rd_url()

        try:
            response = requests.get(
                url = rd_only_mgr_url + "/messages",
                json={
                    "topic_name": args["topic_name"],
                    "consumer_id": args["consumer_id"]
                }
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        if response.status_code != HTTP_200_OK:
            return {
                "status": "Failure",
                "message": "Unable to fetch message due to no more messages or broker is down"
            }, HTTP_400_BAD_REQUEST
        
        return {
                "status": "Success",
                "message": response.json().get("message")
            }, HTTP_200_OK 

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("producer_id", type=int, required=True, help="Producer id required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("message", required=True, help="Message required")
        parser.add_argument("partition_id", type=int)
        args = parser.parse_args()

        wr_only_mgr_url = urlbook.get_wr_url()

        request_dict = {
            "producer_id": args["producer_id"],
            "topic_name": args["topic_name"],
            "message": args["message"],
        }

        if args.get("partition_id"):
            request_dict["partition_id"] = args["partition_id"]
        
        try:
            response = requests.post(
                url = wr_only_mgr_url + "/messages",
                json = request_dict
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        if response.status_code != HTTP_201_CREATED:
            return {
                "status": "Failure",
                "message": "Unable to add message due to invalid producer id or broker is down"
            }, HTTP_400_BAD_REQUEST 
        else:
            return {
                "status": "Success"
            }, HTTP_201_CREATED

class MessageSizeAPI:
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("consumer_id", type=int, required=True, help="Consumer id required")
        args = parser.parse_args()

        rd_only_mgr_url = urlbook.get_random_live_rd_url()

        try:
            response = requests.get(
                url = rd_only_mgr_url + "/unread_messages",
                json={
                    "topic_name": args["topic_name"],
                    "consumer_id": args["consumer_id"]
                }
            )
        except requests.exceptions.RequestException:
            return {
                "status": "Failure",
                "message": "Error Connecting to Manager"
            }, HTTP_400_BAD_REQUEST

        if response.status_code != HTTP_200_OK:
            return {
                "status": "Failure",
                "message": "Unable to fetch message due to no more messages or broker is down"
            }, HTTP_400_BAD_REQUEST
        
        return {
                "status": "Success",
                "message_size": response.json().get("message_size")
            }, HTTP_200_OK 


api.add_resource(TopicAPI, "/topics")
api.add_resource(ProducerAPI, "/producers")
api.add_resource(ConsumerAPI, "/consumers")
api.add_resource(MessageAPI, "/messages")
