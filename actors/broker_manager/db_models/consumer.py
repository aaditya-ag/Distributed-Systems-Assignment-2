from src import db
from db_models.topic import TopicModel


class ConsumerModel(db.Model):
    __tablename__ = "consumer"

    # A Consumer must have an id
    consumer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic for which the consumer registers
    topic = db.Column(db.String, db.ForeignKey(TopicModel.name))

    # Maintain an index upto which the consumer has read the messages
    idx_read_upto = db.Column(db.Integer, default=0)

    def as_dict(self):
        return {
            "consumer_id": self.consumer_id,
            "topic_id": self.topic_id,
            "idx_read_upto": self.idx_read_upto,
        }
