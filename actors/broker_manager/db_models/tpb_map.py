from src import db
from db_models import TopicModel


class TPBMapModel:
    """
    Stores entries as mapping of {Topic(T)--PartitionID(P)--BrokerID(B)}
    """

    __tablename__ = "tpb_map"

    id = db.Column(db.Integer, primary_key=True)
    topic_name = db.Column(db.String, db.ForeignKey(TopicModel.name))
    partition_id = db.Column(db.Integer, nullable=False)
    broker_id = db.Column(db.Integer, nullable=False)
