# In this file, you define all the API endpoints for broker
from flask_restful import Resource, Api, reqparse

from src import (
    app,
    db
)
from db_models import *

from src.http_status_codes import *

api = Api(app)


class Heartbeat(Resource):
    def get(self):
        return {
            "status": "Success",
        }, HTTP_200_OK


class Logs(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("log_index", required=True, help="Log Index Required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("partition_id", required=True, help="Partition id required")

        args = parser.parse_args()

        print(f'{args["log_index"]},{args["topic_name"]},{args["partition_id"]}')

        log = LogModel.query.filter_by(
            log_index=args["log_index"],
            topic_name=args["topic_name"],
            partition_id=args["partition_id"],
        ).first()

        if log is None:
            return {
                "status": "Failure"
            }, HTTP_400_BAD_REQUEST
        else:
            return {
                "status": "Success",
                "log": log.log_message
            }, HTTP_200_OK

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("log_index", required=True, help="Log Index Required")
        parser.add_argument("topic_name", required=True, help="Topic name required")
        parser.add_argument("partition_id", required=True, help="Partition id required")
        parser.add_argument("log_message", required=True, help="Log message required")

        args = parser.parse_args()

        print(
            f'{args["log_index"]},{args["topic_name"]},{args["partition_id"]},{args["log_message"]}'
        )

        log = LogModel(log_index = args["log_index"],
                       topic_name = args["topic_name"],
                       partition_id = args["partition_id"],
                       log_message = args["log_message"])

        db.session.add(log)
        db.session.commit()

        return {
                "status": "Success",
                "message": "Message Successfully created"
            }, HTTP_201_CREATED 


api.add_resource(Heartbeat, "/")
api.add_resource(Logs, "/logs")
