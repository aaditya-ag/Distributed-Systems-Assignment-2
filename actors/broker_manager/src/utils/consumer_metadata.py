import threading

class ConsumerMetaData:
    """
        A dictionary to store all consumers subscribed to a particular topic 
        and their metadata, i.e., the index read upto.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.dict = {}

    def add(self, consumer_id, idx_read_upto):
        self.lock.acquire()
        self.dict[consumer_id] = idx_read_upto
        self.lock.release()

    def create(self):
        self.lock.acquire()
        consumer_id = len(self.dict)
        self.dict[consumer_id] = 0
        self.lock.release()
        return consumer_id
    
    def contains(self, consumer_id):
        self.lock.acquire()
        verdict = False
        if consumer_id in self.dict:
            verdict = True
        self.lock.release()
        return verdict
    
    def get(self, consumer_id):
        self.lock.acquire()
        index = self.dict[consumer_id]
        self.lock.release()
        return index
        
    def update(self, consumer_id):
        self.lock.acquire()
        self.dict[consumer_id] += 1
        print(f'New consumer id: {self.dict[consumer_id]}')
        self.lock.release()

    def get_and_update(self, consumer_id, limit):
        self.lock.acquire()
        index_read_upto = self.dict[consumer_id]
        print(f"idx_read_upto={index_read_upto}")
        if self.dict[consumer_id] < limit:
            self.dict[consumer_id] += 1
        self.lock.release()
        return index_read_upto
        