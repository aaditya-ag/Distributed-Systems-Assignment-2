import threading
import random
from broker import Broker
from utils import TopicToLocationDict, Prounter

class MasterBroker:
    def __init__(self):
        self.lock = threading.Lock()
        self.counter = Prounter()
        self.brokers = {} # dict(broker_id => Broker())
        self.topic_to_partitionBrokerDict = TopicToLocationDict() # dict(topic_name => dict(partition_id => broker_id))

    def add_broker(self, ip, port):
        # WAL Update
        self.lock.acquire()
        broker_id = self.counter.get_post_increment()
        self.brokers[broker_id] = Broker(broker_id, ip, port)
        self.lock.release()
        #DB update
     
    def remove_broker(self, broker_id):
        # WAL Update
        self.lock.acquire()
        if broker_id in self.brokers:
            del self.brokers[broker_id]
        self.lock.release()
        #DB update

    def get_broker(self, topic_name, partition_id):
        broker_id = self.topic_to_partitionBrokerDict.get_broker_id(topic_name, partition_id)
        return self.brokers[broker_id]

    
    def assign_partition(self, topic_name):
        partitionBroker_dict = self.topic_to_partitionBrokerDict.get(topic_name)
        partition_ids = [
            partition_id 
            for partition_id, broker_id in partitionBroker_dict.items()
            if self.brokers[broker_id].is_alive()
        ]
        if len(partition_ids) == 0:
            return None
        return random.choice(partition_ids)
    

    def get_least_loaded_brokers(self):
        least_loaded_brokers = list(self.brokers.values()).sort(key=lambda broker:broker.num_partitions)[:3]
        return least_loaded_brokers

    def add_partitions(self, topic_name, partition_broker_list):
        """
        Params: topic_name, list([broker_id, partition_id])
        """
        for broker_id, partition_id in partition_broker_list:
            self.topic_to_partitionBrokerDict.add(topic_name, broker_id, partition_id)
            self.brokers[broker_id].increase_partition()

    def change_broker_live_status(self, broker_id, current_status):
        self.brokers[broker_id].update_running_status(current_status)

    def is_alive(self, topic_name, partiton_id):
        return self.get_broker(topic_name, partiton_id).is_alive()
    
    def __str__(self):
        return f"MasterBroker containing {len(self.brokers)} Brokers"