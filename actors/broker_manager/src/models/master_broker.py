import threading
import random
from src.models import Broker
from src.utils import Prounter, TopicToLocationDict
from sqlalchemy import func 

from db_models import (
    BrokerModel,
    TPBMapModel,
    TopicModel
)

from src import db

class MasterBroker:
    def __init__(self):
        self.lock = threading.Lock()
        self.counter = Prounter()
        self.brokers = {}  # dict(broker_id => Broker())
        self.topic_to_location = TopicToLocationDict()

    def fetch_from_db(self):
        self.lock.acquire()
        
        if BrokerModel.query.first() is not None:
            self.counter.set(db.session.query(func.max(BrokerModel.id)).scalar())

        brokers = BrokerModel.query.order_by(BrokerModel.id).all()
        for broker in brokers:
            self.add_broker(ip=broker.ip, port=broker.port, is_running=broker.is_running)

        tpb_entries = TPBMapModel.query.all()
        for tpb_entry in tpb_entries:
            self.add_partitions(tpb_entry.topic_name, [tpb_entry.broker_id, tpb_entry.partition_id])

        self.lock.release()

    def add_broker(self, ip, port, is_running=True):
        """
        Add a broker with an ip and port
        id should be consistent with the database ids, hence uses Prounter

        Params:
        -----------------
        ip: str
        port: str

        Returns:
        -----------------
        None
        """
        # WAL Update

        if BrokerModel.query.filter_by(ip=ip, port=port).first() is not None:
            print("WARNING: Attempted to add the same broker twice. Ignoring...")
            return
        
        self.lock.acquire()
        broker_id = self.counter.get_post_increment()
        self.brokers[broker_id] = Broker(
            id=broker_id, 
            ip=ip, 
            port=port, 
            is_running=is_running
        )
        self.lock.release()
        
        # DB update
        broker = BrokerModel(
            id=broker_id,
            ip=ip,
            port=port,
        )
        db.session.add(broker)
        db.session.commit()


    def remove_broker(self, broker_id):
        """
        Remove a broker

        Params:
        -----------------
        ip: str
        port: str

        Returns:
        -----------------
        None
        """
        # WAL Update
        self.lock.acquire()
        for topic_name, partition_id in self.brokers[broker_id].topic_partitions:
            self.topic_to_location.remove(topic_name, partition_id)
        del self.brokers[broker_id]
        self.lock.release()
        
        # DB update
        BrokerModel.query.filter_by(id=broker_id).delete()
        db.session.commit()

    def get_broker(self, topic_name, partition_id):
        """
        Params:
        -----------------
        topic_name: str
        partition_id: int

        Returns:
        -----------------
        a Broker instance of the required broker
        """
        broker_id = self.topic_to_location.get_broker_id(topic_name, partition_id)
        return self.brokers[broker_id]

    def add_partitions(self, topic_name, partition_broker_list):
        """
        Params:
        -----------------
        topic_name: str
        partition_broker_list: list([broker_id:int, partition_id:int])

        Returns:
        -----------------
        None
        """
        for [broker_id, partition_id] in partition_broker_list:
            self.topic_to_location.add(topic_name, broker_id, partition_id)
            self.brokers[broker_id].add_partition(topic_name, partition_id)
        

    def assign_partition(self, topic_name):
        """
        Selects a random partition assigned for this topic from a running broker

        Params:
        -----------------
        topic_name: str

        Returns:
        -----------------
        partition_id: int
        """
        location_dict = self.topic_to_location.get(topic_name)
        partition_ids = [
            partition_id
            for partition_id, broker_id in location_dict.items()
            if self.brokers[broker_id].is_alive()
        ]
        if len(partition_ids) == 0:
            return None
        return random.choice(partition_ids)

    def get_least_loaded_brokers(self):
        """
        Sorts the brokers based on their number of partition present in them
        Returns the 3 Least loaded broker instances

        Params:
        -----------------
        None

        Returns:
        -----------------
        List(Broker)[3]
        """
        least_loaded_brokers = [broker for broker in self.brokers.values()  if broker.is_alive()]
        least_loaded_brokers.sort(key=lambda broker: broker.get_number_of_partitions())
        return [broker.id for broker in least_loaded_brokers[:3]]

    def change_broker_live_status(self, broker_id, is_running):
        """
        Changes status of broker assigned for this topic and partition

        Params:
        -----------------
        broker_id: int
        is_running: boolean

        Returns:
        -----------------
        None
        """
        self.brokers[broker_id].update_running_status(is_running)


    def is_alive(self, topic_name, partiton_id):
        """
        Checks if broker assigned for this topic and partition is alive or not

        Params:
        -----------------
        topic_name: str
        partiton_id: int

        Returns:
        -----------------
        Boolean
        """
        return self.get_broker(topic_name, partiton_id).is_alive()

    def __str__(self):
        return f"MasterBroker containing {len(self.brokers)} Brokers"
