from src import db

class LogModel(db.Model):
    __tablename__ = "log"

    log_index = db.Column(db.Integer, primary_key = True)
    topic_name = db.Column(db.String(64), primary_key = True)
    partition_id = db.Column(db.Integer, primary_key = True)
    log_message = db.Column(db.String(64), nullable = False)

    # def __init__(self, log_index, topic_name, partition_id, log_message):
    #     super().__init__()
    #     self.log_index = log_index
    #     self.topic_name = topic_name
    #     self.partition_id = partition_id
    #     self.log_message = log_message

    def as_dict(self):
        return {
            "log_index": self.log_index,
            "topic_name": self.topic_name,
            "partition_id": self.partition_id,
            "log_message": self.log_message,
        }