import threading

class Broker:
    def __init__(self, id, ip, port):
        self.lock = threading.Lock()
        self.id = id
        self.ip = ip
        self.port = port
        self.num_partitions = 0
        self.is_running = True # Up or down

    def increase_partition(self):
        self.lock.acquire()
        self.num_partitions += 1
        self.lock.release()

    def update_running_status(self, current_status):
        self.lock.acquire()
        self.is_running = current_status
        self.lock.release()

    def is_alive(self):
        return (self.is_running == True)

    def __str__(self):
        return f"Broker://{self.ip}:{self.port}"