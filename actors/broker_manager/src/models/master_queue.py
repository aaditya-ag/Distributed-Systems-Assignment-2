import threading
import requests
from src import db

from src.models import (
    Topic,
    Broker,
    MasterBroker
)

from db_models import (
    TopicModel,
    ProducerModel,
    ConsumerModel,
    TPLMapModel,
    WALModel
)

from src.http_status_codes import *
from utils import wal_utils as WAL

class MasterQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.topics = {} # dict[topic_name: Topic] : Topic class object representing the topic
        self.master_broker = MasterBroker()
        # self.logging = True

    def fetch_from_db(self):
        self.lock.acquire()

        self.master_broker.fetch_from_db()
        topics = TopicModel.query.all()
        for topic in topics:
            self.topics[topic.name] = Topic(topic.name)
        
        consumers = ConsumerModel.query.all()
        for consumer in consumers:
            self.topics[consumer.topic].add_consumer(consumer.consumer_id, consumer.idx_read_upto)
        
        producers = ProducerModel.query.all()
        for producer in producers:
            self.topics[producer.topic].add_producer(producer.producer_id)
        
        logs = TPLMapModel.query.order_by(TPLMapModel.log_index).all()
        for log in logs:
            self.topics[log.topic_name].add_message_index(log.partition_id, log.producer_id)
            
        self.lock.release()

    def add_broker(self, ip, port):
        self.master_broker.add_broker(ip, port)

    def add_topic(self, topic_name):
        # WAL transaction begin
        log_id = WAL.log(tablename="Topic", operation=WAL.INSERT, num_args=1, args=[topic_name], stage=WAL.STATUS_BEGIN)

        brokers = self.master_broker.get_least_loaded_brokers()
        self.lock.acquire()
        if topic_name in self.topics.keys():
            raise Exception("ERROR: topic already exists in the queue.")
        self.topics[topic_name] = Topic(topic_name)
        self.lock.release()

        # DB update
        topic = TopicModel(name=topic_name)
        db.session.add(topic)
        db.session.commit()

        # WAL transaction end
        WAL.log(tablename="Topic", operation=WAL.INSERT, num_args=1, args=[topic_name], stage=WAL.STATUS_END, begin_id=log_id)

        self.master_broker.add_partitions(topic_name, self.do_partition(brokers, num_partitions=len(brokers)))


    def add_producer(self, topic_name):
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            self.add_topic(topic_name)

        producer_id = self.topics[topic_name].register_producer()
        
        # WAL transaction begin
        log_id = WAL.log(tablename="Producer", operation=WAL.INSERT, num_args=2, args=[producer_id, topic_name], stage=WAL.STATUS_BEGIN)

        # DB update
        producer = ProducerModel(
            producer_id=producer_id,
            topic=topic_name
        )
        db.session.add(producer)
        db.session.commit()

        # WAL transaction end
        WAL.log(tablename="Producer", operation=WAL.INSERT, num_args=2, args=[producer_id, topic_name], stage=WAL.STATUS_END, begin_id=log_id)

        return producer_id

    def add_consumer(self, topic_name):
        # WAL transaction begin
        # wal.log(opcode="ADD_CONS", argstring=str(topic_name+"#"))
        
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            return None
        
        consumer_id = self.topics[topic_name].register_consumer()

        # WAL transaction begin
        log_id = WAL.log(tablename="Consumer", operation=WAL.INSERT, num_args=2, args=[consumer_id, topic_name], stage=WAL.STATUS_BEGIN)
        
        # DB update
        consumer = ConsumerModel(
            consumer_id=consumer_id,
            topic=topic_name
        )
        db.session.add(consumer)
        db.session.commit()

        # WAL transaction begin
        WAL.log(tablename="Consumer", operation=WAL.INSERT, num_args=2, args=[consumer_id, topic_name], stage=WAL.STATUS_END, begin_id=log_id)
        
        return consumer_id

    
    def enqueue(self, topic_name, producer_id, message, partition_id = None):
        # WAL transaction begin
        if(not self.has_topic(topic_name)):
            return False
        
        # get broker, partition from master_broker
        broker:Broker = None
        if (partition_id == None):
            assigned_partition_id = self.master_broker.assign_partition(topic_name)
            if (assigned_partition_id is None):
                # raise Exception("ERROR: no alive broker in system.")
                return False
            broker = self.master_broker.get_broker(topic_name, assigned_partition_id)
            partition_id = assigned_partition_id
        else: 
            if( not self.master_broker.is_alive(topic_name, partition_id)):
                # raise Exception("ERROR: broker down.")
                return False
            broker = self.master_broker.get_broker(topic_name, partition_id)
        
        # send update to the in-memory structure
        message_index = self.topics[topic_name].add_message_index(partition_id, producer_id)
        if (message_index < 0):
            # raise Exception("ERROR: producer error.")
            return False
        
        # WAL transaction begin
        log_id = WAL.log(tablename="TPLMap", operation=WAL.INSERT, num_args=4, args=[topic_name, producer_id, partition_id, message_index], stage=WAL.STATUS_BEGIN)

        if not broker.is_alive():
            WAL.log(tablename="TPLMap", operation=WAL.INSERT, num_args=4, args=[topic_name, producer_id, partition_id, message_index], stage=WAL.STATUS_ERR, begin_id=log_id)
            return False
        
        # send request to broker
        broker_log_url = broker.get_base_url() + "/logs"
        response = requests.post(
            url = broker_log_url,
            json={
                "log_index": message_index,
                "topic_name": topic_name,
                "partition_id": partition_id,
                "log_message": message
            }
        )

        if (response.status_code != HTTP_201_CREATED):
            WAL.log(tablename="TPLMap", operation=WAL.INSERT, num_args=4, args=[topic_name, producer_id, partition_id, message_index], stage=WAL.STATUS_ERR, begin_id=log_id)
            return False

        # DB update
        tpl_entry = TPLMapModel(
            topic_name=topic_name,
            producer_id=producer_id,
            partition_id=partition_id,
            log_index=message_index
        )
        db.session.add(tpl_entry)
        db.session.commit()

        WAL.log(tablename="TPLMap", operation=WAL.INSERT, num_args=4, args=[topic_name, producer_id, partition_id, message_index], stage=WAL.STATUS_END, begin_id=log_id)

        return True

    def dequeue(self, topic_name, consumer_id):
        # WAL transaction begin
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            return None
        
        # get partition from the in-memory structure
        index, partition_id = self.topics[topic_name].get_and_update_message_index(consumer_id)
        if(index < 0):
            # raise Exception("ERROR: consumer read error")
            print("Indexerror")
            return None
        
        log_id = WAL.log(tablename="Consumer", operation=WAL.UPDATE, num_args=3, args=[consumer_id, topic_name, index], stage=WAL.STATUS_BEGIN)
            
        # get broker for the partition returned, 
        if (not self.master_broker.is_alive(topic_name, partition_id)):
            # raise Exception("ERROR: broker down")
            WAL.log(tablename="Consumer", operation=WAL.UPDATE, num_args=3, args=[consumer_id, topic_name, index], stage=WAL.STATUS_ERR, begin_id=log_id)
            return None
        
        broker:Broker = self.master_broker.get_broker(topic_name, partition_id)

        if not broker.is_alive():
            print("Broker down")
            WAL.log(tablename="Consumer", operation=WAL.UPDATE, num_args=3, args=[consumer_id, topic_name, index], stage=WAL.STATUS_ERR, begin_id=log_id)
            return None
        
        # send request to broker
        broker_log_url = broker.get_base_url() + "/logs"
        response = requests.get(
            url=broker_log_url,
            json={
                "log_index": index,
                "topic_name": topic_name,
                "partition_id": partition_id,
            }
        )

        print(response.status_code)

        if response.status_code != HTTP_200_OK:
            WAL.log(tablename="Consumer", operation=WAL.UPDATE, num_args=3, args=[consumer_id, topic_name, index], stage=WAL.STATUS_ERR, begin_id=log_id)
            return None
        
        log_message = response.json().get("log")

        print(log_message)
        
        # DB update
        consumer = ConsumerModel.query.filter_by(consumer_id=consumer_id).first()
        consumer.idx_read_upto = index
        db.session.commit()

        WAL.log(tablename="Consumer", operation=WAL.UPDATE, num_args=3, args=[consumer_id, topic_name, index], stage=WAL.STATUS_END, begin_id=log_id)

        return log_message

    def count_unread_messages(self, topic_name, consumer_id):
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            return None

        num_unread_message = self.topics[topic_name].count_unread_messages(consumer_id)
        if num_unread_message == -1:
            return None
        else:
            return num_unread_message

    # def is_logging(self):
    #     return self.logging

    def has_topic(self, str):
        with self.lock:
            return (str in self.topics.keys())

    def do_partition(self, broker_ids, num_partitions=3):
        return [[broker_ids[i%len(broker_ids)], i] for i in range(0, num_partitions)]

    # def sync_db(walentry:WAL.WAL_Entry):
        # self.lock.acquire()
        # WAL.commit(walentry)
        # self.update_from_db()
        # self.lock.release()
    
    # def update_from_db():
    #     for topic in self.topics:
    #         topic.update_from_db()
        
    #     self.master_broker.update_from_db()
