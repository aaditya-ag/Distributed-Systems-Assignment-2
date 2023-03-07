import threading
import requests
from src import db
from datetime import datetime
from src.utils import sync_db
import os

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
)

from src.http_status_codes import *

class MasterQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.topics = {} # dict[topic_name: Topic] : Topic class object representing the topic
        self.master_broker = MasterBroker()
        self.last_checkpoint = datetime.min # when a consistent state was achieved 
        self.last_updated_at = datetime.min # when the last db update was committed

    def create_checkpoint(self):
        print("Checkpointing ... ")
        self.lock.acquire()
        self.update_from_db()
        self.last_checkpoint = self.last_updated_at
        self.lock.release()

    def do_init_sync(self):
        # Hardcoded here, needs change
        wr_only_mgr_url = os.environ.get("WR_ONLY_MGR_URL")
        if wr_only_mgr_url is None:
            wr_only_mgr_url = "http://127.0.0.1:5001"

        # print(os.environ.get("PORT"))
        if os.environ.get("PORT") == wr_only_mgr_url.rsplit(":", 1)[1]:
            return
        
        self.lock.acquire()
        
        response = requests.get(
            url=wr_only_mgr_url + "/init_sync",
            json={
                "timestamp": sync_db.get_minimum_of_max_timestamps_from_all_tables(),
            }
        )

        if response.status_code != HTTP_200_OK:
            self.lock.release()
            return
        
        updates = response.json()["updates"]
        # print(updates)
        for update in updates:
            print("------------------")
            table_name = update[0]
            update_data = update[1]
            print(table_name, update_data)
            sync_db.insert(table_name, update_data)
            self.last_updated_at = max(self.last_updated_at, update_data["updated_at"])
            print("------------------")
        
        self.lock.release()

    def fetch_from_db(self):
        self.lock.acquire()

        present_update_timestamp = self.master_broker.fetch_from_db()
        topics = TopicModel.query.all()
        for topic in topics:
            self.topics[topic.name] = Topic(topic.name)
            present_update_timestamp = max(present_update_timestamp, topic.updated_at)
        
        consumers = ConsumerModel.query.all()
        for consumer in consumers:
            self.topics[consumer.topic].add_consumer(consumer.consumer_id, consumer.idx_read_upto)
            present_update_timestamp = max(present_update_timestamp, consumer.updated_at)
        
        producers = ProducerModel.query.all()
        for producer in producers:
            self.topics[producer.topic].add_producer(producer.producer_id)
            present_update_timestamp = max(present_update_timestamp, producer.updated_at)
        
        logs = TPLMapModel.query.order_by(TPLMapModel.log_index).all()
        for log in logs:
            self.topics[log.topic_name].add_message_index(log.partition_id, log.producer_id)
            present_update_timestamp = max(present_update_timestamp, log.updated_at)

        self.last_updated_at = present_update_timestamp
        self.lock.release()

    def update_from_db(self):
        # NOTE: lock is to be already acquired by the parent caller "create_checkpoint()"
        # self.lock.acquire()
        present_update_timestamp = self.master_broker.update_from_db(last_checkpoint=self.last_checkpoint)
        topics = TopicModel.query.filter(TopicModel.updated_at > self.last_checkpoint).all()
        for topic in topics:
            self.topics[topic.name] = Topic(topic.name)
            present_update_timestamp = max(present_update_timestamp, topic.updated_at)

        consumers = ConsumerModel.query.filter(ConsumerModel.updated_at > self.last_checkpoint).all()
        for consumer in consumers:
            self.topics[consumer.topic].add_consumer(consumer.consumer_id, consumer.idx_read_upto)
            present_update_timestamp = max(present_update_timestamp, consumer.updated_at)

        producers = ProducerModel.query.filter(ProducerModel.updated_at > self.last_checkpoint).all()
        for producer in producers:
            self.topics[producer.topic].add_producer(producer.producer_id)
            present_update_timestamp = max(present_update_timestamp, producer.updated_at)
        
        logs = TPLMapModel.query.filter(TPLMapModel.updated_at > self.last_checkpoint).order_by(TPLMapModel.log_index).all()
        for log in logs:
            self.topics[log.topic_name].add_message_index(log.partition_id, log.producer_id)
            present_update_timestamp = max(present_update_timestamp, log.updated_at)

        self.last_updated_at = present_update_timestamp
        # self.lock.release()

    def add_broker(self, ip, port):
        self.master_broker.add_broker(ip, port)

    def get_brokers(self):
        return self.master_broker.get_brokers()

    def add_topic(self, topic_name):
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

        sync_db.sync_others(operation=sync_db.INSERT, table_name="Topic", data=topic.as_dict())
        
        self.master_broker.add_partitions(topic_name, self.do_partition(brokers, num_partitions=len(brokers)))


    def add_producer(self, topic_name):
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            self.add_topic(topic_name)
            
        producer_id = self.topics[topic_name].register_producer()
        topic_locations = self.master_broker.get_locations_for_topic(topic_name)

        # DB update
        producer = ProducerModel(
            producer_id=producer_id,
            topic=topic_name
        )
        db.session.add(producer)
        db.session.commit()
        sync_db.sync_others(operation=sync_db.INSERT, table_name="Producer", data=producer.as_dict(), checkpoint=True)
        return producer_id, topic_locations

    def add_consumer(self, topic_name):
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            return None
        
        consumer_id = self.topics[topic_name].register_consumer()
        
        # DB update
        consumer = ConsumerModel(
            consumer_id=consumer_id,
            topic=topic_name
        )
        db.session.add(consumer)
        db.session.commit()
        sync_db.sync_others(operation=sync_db.INSERT, table_name="Consumer", data=consumer.as_dict(), checkpoint=True)
        return consumer_id

    
    def enqueue(self, topic_name, producer_id, message, partition_id = None):
        # WAL update
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


        if not broker.is_alive():
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

        sync_db.sync_others(operation=sync_db.INSERT, table_name="TPLMap", data=tpl_entry.as_dict(), checkpoint=True)
        return True

    def dequeue(self, topic_name, consumer_id):
        # WAL update
        if(not self.has_topic(topic_name)):
            # raise Exception("ERROR: topic does not exists.")
            return None
        
        # get partition from the in-memory structure
        index, partition_id = self.topics[topic_name].get_and_update_message_index(consumer_id)
        if(index < 0):
            # raise Exception("ERROR: consumer read error")
            print("Indexerror")
            return None

        # get broker for the partition returned, 
        if (not self.master_broker.is_alive(topic_name, partition_id)):
            # raise Exception("ERROR: broker down")
            return None
        
        broker:Broker = self.master_broker.get_broker(topic_name, partition_id)

        if not broker.is_alive():
            print("Broker down")
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
            return None
        
        log_message = response.json().get("log")

        print(log_message)
        
        # DB update
        consumer = ConsumerModel.query.filter_by(consumer_id=consumer_id).first()
        consumer.idx_read_upto = index
        db.session.commit()

        sync_db.sync_others(operation=sync_db.UPDATE, table_name="Consumer", data=consumer.as_dict(), checkpoint=True)

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

    def has_topic(self, str):
        with self.lock:
            return (str in self.topics.keys())

    def do_partition(self, broker_ids, num_partitions=3):

        res= list(
            [broker_ids[i%len(broker_ids)], i] 
            for i in range(0, num_partitions)
        )
        print(res)
        return res

