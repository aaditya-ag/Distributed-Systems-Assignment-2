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
        print(f"Registered Producer: {producer_id}")
        return producer_id
    
    def add_producer(self, producer_id):
        self.producers.add(producer_id)
    
    def get_and_update_message_index(self, consumer_id):
        """
            Given a consumer id, the function returns the tuple (index, partition_id)
            if the log and consumer_id exists, otherwise it returns (-1, -1).
        """
        if not self.consumers.contains(consumer_id):
            print(consumer_id)
            print("Consumer nahi hai", self.name)
            return (-1, -1)
        log_size = self.logs.size()
        index = self.consumers.get_and_update(consumer_id, log_size)
        if index == log_size:
            print("Pura padh liya", self.name)
            return (-1, -1)
        partition_id = self.logs.get(index)

        return (index, partition_id)
    
    def get_message_index(self, consumer_id):
        """
            Given a consumer id, the function returns the tuple (index, partition_id)
            if the log and consumer_id exists, otherwise it returns (-1, -1).
        """
        if not self.consumers.contains(consumer_id):
            print(f'No consumer with id {consumer_id} in {self.name}')
            return (-1, -1) 
        log_size = self.logs.size()
        index = self.consumers.get(consumer_id)
        if index == log_size:
            return (-1, -1)
        partition_id = self.logs.get(index)

        return (index, partition_id)
    
    def update_message_index(self, consumer_id):
        print(f'Updating message index for {consumer_id} in {self.name}')
        # assert self.consumers.contains(consumer_id)
        self.consumers.update(consumer_id)
    
    def add_message_index(self, partition_id, producer_id):
        """
            Given a consumer id, the function adds metadata about log, i.e., the partition id
            to log metadata, and returns the index of that entry in topic queue. 

            Returns -1 if producer doesn't exist.
        """
        if self.producers.contains(producer_id) == False:
            return -1
        index = self.logs.add(partition_id)
        print(f"Topic Model, add_message_index(): {index}")
        return index
    
    def insert_message_index(self, partition_id, producer_id, index):
        if self.producers.contains(producer_id) == False:
            return -1
        self.logs.insert(partition_id, index)
        return index
    
    def count_unread_messages(self, consumer_id):
        if not self.consumers.contains(consumer_id):
            return -1
        num_read_message = self.get_message_index(consumer_id)

        if num_read_message[0] == -1:
            return 0

        num_unread_message = self.logs.size() - num_read_message[0]
        return num_unread_message