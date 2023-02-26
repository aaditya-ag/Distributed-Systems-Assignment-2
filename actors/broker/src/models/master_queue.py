import threading
from src.models import Topic
from src.models import prounter
class MasterQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.topics = {} # dict[topic_name:str] : Topic class object representing the topic
        self.master_broker = MasterBroker()
        self.num_producer: prounter = prounter(0)
        self.num_consumer: prounter = prounter(0)

    def fetch_from_db(self):
        pass

    def add_topic(self, topic_name: str):
        # WAL update
        # wal.log(opcode="ADD_TOPIC", argstring=str(topic_name+"#"))
        brokers = self.master_broker.get_least_loaded_brokers()
        self.lock.acquire()
        if topic_name in self.topics.keys():
            raise Exception("ERROR: topic already exists in the queue.")
        self.topics[topic_name] = Topic(topic_name, brokers)
        self.lock.release()

        # DB update

    def add_producer(self, topic_name: str):
        # WAL update
        # wal.log(opcode="ADD_PROD", argstring=str(topic_name+"#"))
        
        self.topics[topic_name].add_producer(self.num_producer.get_post_increment())
        return

    def remove_producer(self,):
        pass

    def add_consumer(self, topic_name: str):
        # WAL update
        # wal.log(opcode="ADD_PROD", argstring=str(topic_name+"#"))
        
        self.topics[topic_name].add_consumer(self.num_consumer.get_post_increment())
        return

    def remove_consumer(self,):
        pass
    
    def find_size(self,):
        pass
