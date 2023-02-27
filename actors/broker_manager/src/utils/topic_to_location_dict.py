import threading
from utils import PartitionToBrokerDict


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

    def remove(self, topic_name, partition_id):
        self.lock.acquire()
        self.dict[topic_name].remove(partition_id)
        self.lock.release()

    def get(self, topic_name):
        return self.dict[topic_name]

    def get_broker_id(self, topic_name, partition_id):
        return self.dict[topic_name].get(partition_id)

    def update(self, topic_name, partition_id, broker_id):
        self.dict[topic_name].update(partition_id, broker_id)

    def keys(self):
        return self.dict.keys()

    def values(self):
        return self.dict.values()

    def items(self):
        return self.dict.items()

    def __str__(self):
        return self.items()
