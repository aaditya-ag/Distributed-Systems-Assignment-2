import threading
from broker import Broker

class MasterBroker:
    def __init__(self):
        self.lock = threading.Lock()
        self.brokers = {} # dict(broker_id => Broker())

    def add_broker(self, broker:Broker):
        # WAL Update
        self.lock.acquire()
        self.brokers[broker.id] = broker
        self.lock.release()
        #DB update
     
    def remove_broker(self, broker_id):
        # WAL Update
        self.lock.acquire()
        if broker_id in self.brokers:
            del self.brokers[broker_id]
        self.lock.release()
        #DB update

    def get_least_loaded_brokers(self):
        least_loaded_brokers = list(self.brokers.values()).sort(key=lambda broker:broker.num_partitions)[:3]
        return least_loaded_brokers

    def __str__(self):
        return f"MasterBroker containing {len(self.brokers)} Brokers"