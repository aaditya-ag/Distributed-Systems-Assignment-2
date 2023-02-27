import threading
from partition_to_broker_dict import PartitionToBrokerDict

class TopicToLocationDict:
    """
    A dictionary storing mapping of topic_name to PartitionToBrokerDict
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.dict = {}
    
    def add(self, topic_name, partition_id, broker_id):
        
        self.lock.acquire()
        if topic_name not in self.dict:
            self.dict[topic_name] = PartitionToBrokerDict()
        self.lock.release()

        self.dict[topic_name].add(partition_id, broker_id)
        
    def get(self, topic_name):
        return self.dict[topic_name]
    
    def get_broker_id(self, topic_name, partition_id):
        return self.dict[topic_name].get(partition_id)

    def update(self, topic_name, partition_id, broker_id):
        self.dict[topic_name].update(partition_id, broker_id)
