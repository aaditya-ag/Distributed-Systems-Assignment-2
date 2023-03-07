from src import db
from db_models.topic import TopicModel


class ConsumerModel(db.Model):
    __tablename__ = "consumer"

    # A Consumer must have an id
    consumer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic for which the consumer registers
    topic = db.Column(db.String, db.ForeignKey(TopicModel.name), primary_key=True)

    # Maintain an index upto which the consumer has read the messages
    idx_read_upto = db.Column(db.Integer, default=-1)

    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now(), nullable=False)
    
    __table_args__ = tuple(
        db.UniqueConstraint("consumer_id", "topic", name="consumer_id_constraint")
    )

    def as_dict(self):
        return {
            "consumer_id": self.consumer_id,
            "topic": self.topic,
            "idx_read_upto": self.idx_read_upto,
            "updated_at": self.updated_at.isoformat()
        }
