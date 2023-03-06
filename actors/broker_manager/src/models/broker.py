import threading

from db_models import (
    BrokerModel,
    TPBMapModel
)

from src import db

from utils import wal_utils as WAL

class Broker:
    def __init__(self, id, ip, port, is_running=True):
        self.lock = threading.Lock()
        self.id = id
        self.ip = ip
        self.port = port
        self.topic_partitions = set()  # set of (topic_name, partition_id) in this broker
        self.is_running = is_running  # Alive Status

    def add_partition(self, topic_name, partition_id):
        """
        Add The topic and its corresponding partition
        """
        log_id = WAL.log(table="TPBMap", operation=WAL.INSERT, num_args=3, args=[topic_name, partition_id, self.id], stage=WAL.STATUS_BEGIN)
        self.lock.acquire()
        self.topic_partitions.add((topic_name, partition_id))
        self.lock.release()

        # DB Update
        tpb_entry = TPBMapModel(
            topic_name=topic_name,
            partition_id=partition_id,
            broker_id=self.id
        )
        db.session.add(tpb_entry)
        db.session.commit()
        WAL.log(table="TPBMap", operation=WAL.INSERT, num_args=3, args=[topic_name, partition_id, self.id], stage=WAL.STATUS_END, begin_id=log_id)



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
        BrokerModel.query.filter_by(id=self.id).update(is_running=is_running)
        db.session.commit()


    def is_alive(self):
        """
        Check if broker is alive or not
        """
        return self.is_running
    
    def get_base_url(self):
        return self.ip + ":" + self.port

    def __str__(self):
        return f"Broker://{self.ip}:{self.port}"
