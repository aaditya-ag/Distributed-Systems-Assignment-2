import threading

from db_models import (
    BrokerModel,
    TPBMapModel
)

from src import db

class Broker:
    def __init__(self, id, ip, port, is_running=True):
        self.lock = threading.Lock()
        self.id = id
        self.ip = ip
        self.port = port
        self.topic_partitions = set()  # set of (topic_name, partition_id) in this broker
        self.is_running = is_running  # Alive Status

    def add_partition(self, topic_name, partition_id, mem_only=False):
        """
        Add The topic and its corresponding partition
        """
        self.lock.acquire()
        self.topic_partitions.add((topic_name, partition_id))
        self.lock.release()

        # DB Update
        if mem_only:
            return
        
        tpb_entry = TPBMapModel(
            topic_name=topic_name,
            partition_id=partition_id,
            broker_id=self.id
        )
        db.session.add(tpb_entry)
        db.session.commit()


    def get_number_of_partitions(self):
        """
        Return the number of stored topic-partitions
        """
        return len(self.topic_partitions)

    def update_running_status(self, is_running):
        """
        Update status of broker
        """
        self.lock.acquire()
        self.is_running = is_running
        self.lock.release()

        # DB update
        broker = BrokerModel.query.filter_by(id=self.id).first()
        broker.is_running=is_running
        db.session.commit()


    def is_alive(self):
        """
        Check if broker is alive or not
        """
        return self.is_running
    
    def get_base_url(self):
        return self.ip + ":" + self.port

    def __str__(self):
        return f"Broker:: {self.ip}:{self.port}"
