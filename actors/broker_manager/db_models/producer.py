from src import db
from db_models.topic import TopicModel


class ProducerModel(db.Model):
    __tablename__ = "producer"

    # A Producer must have an id
    producer_id = db.Column(db.Integer, primary_key=True)

    # We need a topic_name for which this producer is registering
    topic = db.Column(db.String, db.ForeignKey(TopicModel.name))

    def as_dict(self):
        return {"producer_id": self.producer_id, "topic_id": self.topic_id}
