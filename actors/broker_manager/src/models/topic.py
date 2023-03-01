from src.utils import (
    ConsumerMetaData,
    LogMetadataQueue,
    # PartitionDict,
    ProducerMetadata
)

class Topic:
    """
        This class stores all metadata related to a particular topic
    """
    def __init__(self, name):
        self.name = name
        self.consumers = ConsumerMetaData()
        self.logs = LogMetadataQueue()
        self.producers = ProducerMetadata()

    def register_consumer(self):
        consumer_id = self.consumers.create()
        return consumer_id
    
    def add_consumer(self, consumer_id, idx_read_upto):
        self.consumers.add(consumer_id, idx_read_upto)
    
    def register_producer(self):
        producer_id = self.producers.create()
        return producer_id
    
    def add_producer(self, producer_id):
        self.producers.add(producer_id)
    
    def get_message_index(self, consumer_id):
        """
            Given a consumer id, the function returns the tuple (index, partition_id)
            if the log and consumer_id exists, otherwise it returns (-1, -1).
        """
        if not self.consumers.contains(consumer_id):
            return (-1, -1)
        log_size = self.logs.size()
        index = self.consumers.read_and_update(consumer_id, log_size)
        if index == log_size:
            return (-1, -1)
        partition_id = self.logs.get(index)

        return (index, partition_id)
    
    def add_message_index(self, partition_id, producer_id):
        """
            Given a consumer id, the function adds metadata about log, i.e., the partition id
            to log metadata, and returns the index of that entry in topic queue. 

            Returns -1 if producer doesn't exist.
        """
        if not self.producers.contains(producer_id):
            return -1
        index = self.logs.add(partition_id)
        return index
    
    def count_unread_messages(self, consumer_id):
        if not self.consumers.contains(consumer_id):
            return -1
        num_read_message = self.get_message_index(consumer_id)
        with self.lock:
            num_unread_message = len(self.logs) - num_read_message
        return num_unread_message