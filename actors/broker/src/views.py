# In this file, you define all the API endpoints for broker
from flask_restful import Resource, Api, reqparse

from src import app

api = Api(app)

class Heartbeat(Resource):
    def get(self):
        return {
            "status": "Success",
        }, 200
    
api.add_resource(Heartbeat, "/")