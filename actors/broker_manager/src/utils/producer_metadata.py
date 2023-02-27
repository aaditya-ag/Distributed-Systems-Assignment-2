import threading

class ProducerMetadata:
    """
        A set to store all producers subscribed to a particular topic
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.set = set()

    def create(self):
        self.lock.acquire()
        producer_id = len(self.set)
        self.set.add(producer_id)
        self.lock.release()
        return producer_id
    
    def contains(self, producer_id):
        self.lock.acquire()
        verdict = False
        if producer_id in self.set:
            verdict = True
        self.lock.release()
        return verdict
