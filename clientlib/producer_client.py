import requests
import aiohttp
import json
from async_socket import AsyncSocket
import routes

class ProducerClient():
    def __init__(self, loadBalancerAddr, loadBalancerPort):
        self.topics = {}
        # self.topic_partitions = {}
        self.loadBalancerURL = "http://"+loadBalancerAddr+":"+loadBalancerPort+"/"
        self.asyncSocket = AsyncSocket()

        # lst = [self.broker]
        # lst.extend(['producer', 'register'])
        # self.url = '/'.join(lst)

        # for topic in topics:
        #     payload = {"topic": topic}
        #     result = post(url, payload)
        #     if result['status'] == "Success":
        #         self.topics[topic] = result['producer_id']
        #     else:
        #         raise Exception(result['message'])

    async def aRegister(self, session, topic_name):
        if topic_name not in self.topics:
            try:
                url = self.loadBalancerURL + routes.REGISTER_PRODUCER
                data = {
                    "topic_name": topic_name,
                }
                
                async with session.post(url, json=data) as response:
                    status = response.status
                    response_json = await response.json()
                    
                    if status == 201:
                        producer_id = response_json["producer_id"]
                        partitions = response_json["topic_locations"]
                        self.topics[topic_name] = (producer_id, partitions)
                        return 0, "Registration success"
                    elif status == 400:
                        return -1, response_json["message"]
                    else:
                        return -1, await response.text()
            except Exception as err:
                return -2, str(err)
        return 0, "Already registered."
    
    def register(self, topic):
        return self.asyncSocket.run(
            self.aRegister, [{"topic_name": topic}]
        )[0]
    
    async def aProduce(self, session, topic_name, message, partition_id=None):
        if topic_name in self.topics:
            try:
                if partition_id is not None and partition_id not in self.topics[topic_name][1]:
                    print("WARNING: given partition not detected, default partition to be used")
                    partition_id = None
                
                url = self.loadBalancerURL + routes.ENQUEUE
                json = {
                    "topic_name": topic_name,
                    "producer_id": self.topics[topic_name][0],
                    "message": message,
                }
                #! confirmation requested
                if partition_id is not None:
                    json["partition_id"] = partition_id
                
                async with session.post(url, json=json) as response:
                    status = response.status
                    response_json = await response.json()

                    if status == 201:
                        return 0, "Message entry created."
                    elif status == 400:
                        return -1, response_json["message"]
                    else:
                        return -1, await response.text()
            except Exception as err:
                return -2, str(err)
        return -1, "Topic not registered."
    
    def display_topic_locations(self, topic):
        if topic in self.topics:
            print("Topic Locations for \"",topic,"\":")
            print(self.topics[topic][1])
        else:
            print("Topic does not exists or is not registered.")

    def send(self, topic, message, partition=None):
        return self.asyncSocket.run(
            self.aProduce, [{"topic_name":topic, "message":message, "partition_id":partition}]
        )[0]
    
    def exit(self):
        self.asyncSocket.close()
        return
