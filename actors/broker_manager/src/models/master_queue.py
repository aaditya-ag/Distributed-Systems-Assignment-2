import threading
from src.models import Topic

class MasterQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.topics = {} # dict[topic_name:str] : Topic class object representing the topic
        self.master_broker = MasterBroker()

    def fetch_from_db(self):
        pass

    def add_topic(self, topic_name):
        # WAL update
        # wal.log(opcode="ADD_TOPIC", argstring=str(topic_name+"#"))
        brokers = self.master_broker.get_least_loaded_brokers()
        self.lock.acquire()
        if topic_name in self.topics.keys():
            raise Exception("ERROR: topic already exists in the queue.")
        self.topics[topic_name] = Topic(topic_name)
        self.lock.release()
        self.master_broker.add_partition(topic_name, self.do_partition(brokers))
        # DB update

    def add_producer(self, topic_name):
        # WAL update
        # wal.log(opcode="ADD_PROD", argstring=str(topic_name+"#"))
        if(not self.has_topic(topic_name)):
            raise Exception("ERROR: topic does not exists.")
        self.topics[topic_name].register_producer()
        # DB update
        return

    def add_consumer(self, topic_name):
        # WAL update
        # wal.log(opcode="ADD_CONS", argstring=str(topic_name+"#"))
        if(not self.has_topic(topic_name)):
            raise Exception("ERROR: topic does not exists.")
        self.topics[topic_name].register_consumer()
        # DB update
        return

    def enqueue(self, topic_name, producer_id, message, partition_id = None):
        # WAL update
        if(not self.has_topic(topic_name)):
            raise Exception("ERROR: topic does not exists.")
        # get broker, partition from master_broker
        if (partition_id == None):
            assigned_partition_id = self.master_broker.assign_partition(topic_name)
            if(assigned_partition_id is None):
                raise Exception("ERROR: no alive broker in system.").
            broker_details = self.master_broker.get_broker(topic_name, assigned_partition_id)
            partition_id = assigned_partition_id
        else: 
            if( not self.master_broker.is_alive(topic_name, partition_id)):
                raise Exception("ERROR: broker down.")
            broker_details = self.master_broker.get_broker(topic_name, partition_id)
        
        # send update to the in-memory structure
        message_index = self.topics[topic_name].add_message_index(partition_id, producer_id)
        if (message_index < 0):
            raise Exception("ERROR: producer error.")

        # send request to broker

        # DB update
        return

    def dequeue(self, topic_name, consumer_id):
        # WAL update
        if(not self.has_topic(topic_name)):
            raise Exception("ERROR: topic does not exists.")
        # get partition from the in-memory structure
        index, partition_id = self.topics[topic_name].get_message_index(consumer_id)
        if(index < 0):
            raise Exception("ERROR: consumer read error")

        # get broker for the partition returned, 
        if (not self.master_broker.is_alive(topic_name, partition_id)):
            raise Exception("ERROR: broker down")
        broker_details = self.master_broker.get_broker(topic_name, partition_id)

        # send request to broker
        
        # DB update
        return

    def find_size(self,):
        pass  

    def has_topic(self, str):
        with self.lock:
            return (str in self.topics.keys())

    def do_partition(self, broker_ids, num_partitions=3):
        return [[broker_ids[i%len(broker_ids)], i] for i in range(0, num_partitions)]

