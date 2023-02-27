import threading


class PartitionToBrokerDict:
    """
    A dictionary storing mapping of partition id to broker ids
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.dict = {}

    def add(self, partition_id, broker_id):
        self.lock.acquire()
        self.dict[partition_id] = broker_id
        self.lock.release()

    def remove(self, partition_id):
        self.lock.acquire()
        del self.dict[partition_id]
        self.lock.release()

    def get(self, partition_id):
        return self.dict[partition_id]

    def update(self, partition_id, broker_id):
        self.lock.acquire()
        self.dict[partition_id] = broker_id
        self.lock.release()

    def keys(self):
        return self.dict.keys()

    def values(self):
        return self.dict.values()

    def items(self):
        return self.dict.items()

    def __str__(self):
        return self.items()
