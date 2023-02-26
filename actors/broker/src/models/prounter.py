import threading
class prounter:
    def __init__(self, init_value: int= 0) -> None:
        self.value: int = init_value
        self.lock = threading.Lock()
    
    def get(self) -> int:
        with self.lock:
            return self.value
        
    def get_post_increment(self) -> int:
        with self.lock:
            current_value = self.value
            self.value += 1
        return current_value
    
    def to_str(self) -> str:
        return f"Value({self.value})"

