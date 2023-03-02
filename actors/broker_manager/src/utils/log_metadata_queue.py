import threading

class LogMetadataQueue:
    """
        A list to store mapping of log's paritions and it's metadata.
    """
    
    def __init__(self):
        self.lock = threading.Lock()
        self.queue = []

    def add(self, partition_id):
        self.lock.acquire()
        index = len(self.queue)
        self.queue.append(partition_id)
        self.lock.release()
        print(f"Log Metadata Queue index: {index}")
        return index
    
    def get(self, index):
        return self.queue[index]
    
    def size(self):
        self.lock.acquire()
        size = len(self.queue)
        self.lock.release()
        return size

