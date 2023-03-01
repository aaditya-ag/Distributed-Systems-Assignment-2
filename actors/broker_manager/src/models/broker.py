import threading


class Broker:
    def __init__(self, id, ip, port):
        self.lock = threading.Lock()
        self.id = id
        self.ip = ip
        self.port = port
        self.topic_partitions = {}  # set of (topic_name, partition_id) in this broker
        self.is_running = True  # Alive Status

    def add_partition(self, topic_name, partition_id):
        """
        Add The topic and its corresponding partition
        """
        self.lock.acquire()
        self.topic_partitions.add((topic_name, partition_id))
        self.lock.release()

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

    def is_alive(self):
        """
        Check if broker is alive or not
        """
        return self.is_running

    def __str__(self):
        return f"Broker://{self.ip}:{self.port}"
