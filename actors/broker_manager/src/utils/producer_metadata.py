import threading

class ProducerMetadata:
    """
        A set to store all producers subscribed to a particular topic
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.set = set()

    def create(self):
        with self.lock:
            producer_id = len(self.set)
            self.set.add(producer_id)
        return producer_id
    
    def contains(self, producer_id):
        with self.lock:
            if producer_id in self.set:
                return True
            else:
                return False
