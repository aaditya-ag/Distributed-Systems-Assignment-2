from src import db
from db_models.topic import TopicModel


class ProducerModel(db.Model):
    __tablename__ = "producer"

    # A Producer must have an id
    producer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic_name for which this producer is registering
    topic = db.Column(db.String, db.ForeignKey(TopicModel.name), primary_key=True)

    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now(), nullable=False)
    
    __table_args__ = tuple(
        db.UniqueConstraint("producer_id", "topic", name="producer_id_constraint")
    )

    def as_dict(self):
        return {
            "producer_id": self.producer_id, 
            "topic": self.topic,
            "updated_at": self.updated_at.isoformat()
        }
