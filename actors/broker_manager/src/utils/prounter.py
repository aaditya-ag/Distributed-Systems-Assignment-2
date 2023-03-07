
import threading
class Prounter:
    def __init__(self, init_value: int= 0) -> None:
        self.value: int = init_value
        self.lock = threading.Lock()
    
    def get(self) -> int:
        with self.lock:
            return self.value
        
    def set(self, value):
        with self.lock:
            self.value = value
        
    def get_post_increment(self) -> int:
        with self.lock:
            self.value += 1
            current_value = self.value
        return current_value
    
    def to_str(self) -> str:
        return f"Value({self.value})"