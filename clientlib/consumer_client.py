import requests
import aiohttp
from async_socket import AsyncSocket
import routes
def get(url, payload):
    response = requests.get(url, json=payload)

    if response.status_code == 200:
        data = response.json()
        return 0, data
    else:
        return -1, {
            "status": "Failure",
            "message": "Request failed with status code: " + str(response.status_code)
        }

def post(url, payload):
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        data = response.json()
        return 0, data
    else:
        return -1, {
            "status": "Failure",
            "message": "Request failed with status code: " + str(response.status_code)
        }

class ConsumerClient():
    def __init__(self, loadBalancerAddr, loadBalancerPort):
        self.topics = {}
        self.loadBalancerURL = "http://"+loadBalancerAddr+":"+loadBalancerPort+"/"
        self.asyncSocket = AsyncSocket()    
    
    async def aRegister(self, session, topic_name):
        if topic_name not in self.topics:
            try:
                url = self.loadBalancerURL + routes.REGISTER_CONSUMER
                data = {
                    "topic_name": topic_name,
                }
                async with session.post(url, json=data) as response:
                    status = response.status
                    response_json = await response.json()
                    
                    if status == 201:
                        consumer_id = response_json["consumer_id"]
                        self.topics[topic_name] = consumer_id
                        return 0, "Registration successful."
                    elif status == 400:
                        return -1, response_json["message"]
                    else:
                        return -1, await response.text()
            except Exception as err:
                return -2, str(err)
        else:
            return 0, "Topic already registered."
    
    def register(self, topic):
        return self.asyncSocket.run(
            self.aRegister, [{"topic_name":topic}]
        )[0]

    async def aSize(self, session, topic_name):
        if topic_name in self.topics:
            try:
                url = self.loadBalancerURL + routes.SIZE
                data = {
                    "topic_name": topic_name,
                    "consumer_id": self.topics[topic_name]
                }
                async with session.get(url, json=data) as response:
                    status = response.status
                    response_json = await response.json()
                    # print("Response_JSON:",response_json)
                    if status == 200:
                        return 0, response_json["message_size"]
                    elif status == 400:
                        return -1, response_json["message"]
                    else:
                        return -1, await response.text()
            except Exception as err:
                return -2, str(err)
        else:
            return -2, "Topic not registered."

    def size(self, topic):
        return self.asyncSocket.run(
            self.aSize, [{"topic_name":topic}]
        )[0]

    def has_next(self, topic):
        status, size = self.size(topic)
        return int(status) >= 0 and int(size) > 0

    def get_next(self, topic):
        url = self.loadBalancerURL + routes.DEQUEUE
        payload = {
            "topic_name": topic,
            "consumer_id": self.topics[topic],
        }
        status, result = get(url, payload)
        if int(status) < 0:
            raise Exception(result['message'])
        return status, result['message']
    
    def exit(self):
        self.asyncSocket.close()
        return