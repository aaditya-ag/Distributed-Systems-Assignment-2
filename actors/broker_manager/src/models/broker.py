import threading

class Broker:
    def __init__(self, id, ip, port, num_partitions):
        self.lock = threading.Lock()
        self.id = id
        self.ip = ip
        self.port = port
        self.num_partitions = num_partitions
        self.is_running = False # Up or down

    def __str__(self):
        return f"Broker://{self.ip}:{self.port}"