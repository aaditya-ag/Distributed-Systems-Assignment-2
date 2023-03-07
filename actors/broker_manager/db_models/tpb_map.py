from src import db
from db_models import TopicModel


class TPBMapModel(db.Model):
    """
    Stores entries as mapping of {Topic(T)--PartitionID(P)--BrokerID(B)}
    """

    __tablename__ = "tpb_map"

    id = db.Column(db.Integer, primary_key=True)
    topic_name = db.Column(db.String, db.ForeignKey(TopicModel.name))
    partition_id = db.Column(db.Integer, nullable=False)
    broker_id = db.Column(db.Integer, nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now(), nullable=False)

    def as_dict(self):
        return {
            "id": self.id,
            "topic_name": self.topic_name,
            "partition_id": self.partition_id,
            "broker_id": self.broker_id,
            "updated_at": self.updated_at.isoformat()
        }