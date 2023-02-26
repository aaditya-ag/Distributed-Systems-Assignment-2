import threading

class ConsumerMetaData:
    """
        A dictionary to store all consumers subscribed to a particular topic 
        and their metadata, i.e., the index read upto.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.dict = {}

    def create(self):
        with self.lock:
            consumer_id = len(self.dict)
            self.dict[consumer_id] = 0
        return consumer_id
    
    def get(self, consumer_id):
        with self.lock:
            return self.dict[consumer_id]
        
    def read_and_update(self, consumer_id, limit):
        with self.lock:
            index_read_upto = self.dict[consumer_id]
            if self.dict[consumer_id] < limit:
                self.dict[consumer_id] += 1
        return index_read_upto
        