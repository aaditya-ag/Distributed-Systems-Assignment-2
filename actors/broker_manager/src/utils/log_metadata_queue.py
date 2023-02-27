import threading

class LogMetadataQueue:
    """
        A list to store mapping of log's paritions and it's metadata.
    """
    
    def __init__(self):
        self.lock = threading.Lock()
        self.queue = []

    def add(self, partition_id):
        with self.lock:
            index = len(self.queue)
            self.queue.append(partition_id)
        return index
    
    def get(self, index):
        return self.queue[index]
    
    def size(self):
        with self.lock:
            size = len(self.queue)
        return size

